#!/bin/bash

worker_main() {
	declare "$@" >&- 2>&-
	declare set_vars=(${@%%=*} broker worker capacity logfile)

	declare broker=${broker-broker}
	declare broker=(${broker//:/ })
	declare worker=${worker-worker-1}
	declare capacity=${capacity-$(nproc)}
	declare logfile=${logfile}
	declare plugins=${plugins}
	xargs_eval -d: source {} >&- 2>&- <<< $plugins

	log "worker version 2022-07-30 (protocol 0)"
	args_of "${set_vars[@]}" | xargs_eval log "option:"
	envinfo | xargs_eval log "platform"

	declare -A own # [id]=owner
	declare -A cmd # [id]=command
	declare -A res # [id]=code:output
	declare -A pid # [id]=PID
	declare -A stdin # [id]=FD
	declare -A stdout # [id]=output

	declare id_next
	declare io_count
	declare tcp_fd
	declare res_fd

	while init_system_io "$@" && init_system_fd "$@"; do
		worker_routine "$@"
		local code=$?
		(( $code < 16 )) && break
	done

	return $code
}

worker_routine() {
	local state=() # (idle|busy #requests/capacity)
	local linked=() # broker...

	log "verify chat system protocol 0..."
	echo "protocol 0"

	local regex_request="^(\S+) >> request (([0-9]+) )?(\{(.*)\}( with( ([^{}]+)| ?))?|(.+))$"
	local regex_confirm="^(\S+) >> (confirm|accept|reject) (state|response|protocol) (.+)$"
	local regex_terminate="^(\S+) >> terminate (\S+)$"
	local regex_others="^(\S+) >> (operate|shell|set|unset|use|query|report) (.+)$"
	local regex_chat_system="^(#|%) (.+)$"
	local regex_request_input="^input (\{(.*)\}|(.+))$"

	local message
	while input message; do
		if [[ $message =~ $regex_confirm ]]; then
			# ^(\S+) >> (confirm|accept|reject) (state|response|protocol) (.+)$
			local who=${BASH_REMATCH[1]}
			local confirm=${BASH_REMATCH[2]}
			local what=${BASH_REMATCH[3]}
			local option=${BASH_REMATCH[4]}

			if [ "$confirm $what" == "confirm state" ]; then
				log "$who confirmed state $option"

			elif [ "$what" == "response" ]; then
				local id=$option

				if [[ -v res[$id] ]] && [ "${own[$id]}" == "$who" ]; then
					if [ "$confirm" == "accept" ]; then
						log "confirm that $who ${confirm}ed response $id"
						unset own[$id] cmd[$id] res[$id] stdin[$id] stdout[$id]
					elif [ "$confirm" == "confirm" ] && [[ -v stdout[$id] ]]; then
						log "confirm that $who ${confirm}ed response $id output"
					elif [ "$confirm" == "reject" ]; then
						log "confirm that $who ${confirm}ed response $id"
						unset res[$id]
						local owner=$who command=${cmd[$id]}
						local options=
						[[ -v stdin[$id] ]] && options+="input "
						[[ -v stdout[$id] ]] && options+="output "
						if confirm_request $id && execute_request $id; then
							echo "$owner << accept request $id"
							log "accept request $id {$command} ${options:+with $options}from $owner, execute $id..."
						else
							unset own[$id] cmd[$id] pid[$id] stdin[$id] stdout[$id]
							echo "$owner << reject request $id"
							log "reject request $id {$command} ${options:+with $options}from $owner due to policy/execution"
						fi
					fi
				elif [[ -v stdout[$id] ]] && [ "${own[$id]}" == "$who" ]; then
					log "confirm that $who ${confirm}ed response $id output"
				elif [ "${own[$id]:-$who}" != "$who" ]; then
					log "ignore that $who ${confirm}ed response $id since it is owned by ${own[$id]}"
				else
					log "ignore that $who ${confirm}ed response $id since no such response"
				fi

			elif [ "$what" == "protocol" ]; then
				if [ "$confirm" == "accept" ]; then
					log "handshake with $who successfully"
					erase_from linked $who
					linked+=($who)
					observe_state; notify_state $who

				elif [ "$confirm" == "reject" ]; then
					log "handshake failed, unsupported protocol; shutdown"
					return 2
				fi

			else
				log "ignore that $who $confirm $what $option from $who"
			fi

		elif [[ $message =~ $regex_request ]]; then
			# ^(\S+) >> request (([0-9]+) )?(\{(.*)\}( with( ([^{}]+)| ?))?|(.+))$
			local owner=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[3]}
			local command=${BASH_REMATCH[5]:-${BASH_REMATCH[9]}}
			local options=${BASH_REMATCH[8]}

			local -A opts=()
			extract_options input output

			if [ "$state" == "idle" ] && [[ ! -v own[$id] ]] && [[ ! $options ]] && \
					( [[ ! $request_whitelist ]] || contains broker $owner ); then
				local request="$id"
				if [[ ! $id ]]; then
					id=${id_next:-1}
					while [[ -v own[$id] ]]; do id=$((id+1)); done
					request="$id {$command}"
				fi
				own[$id]=$owner
				cmd[$id]=$command
				if [[ -v opts[input] ]]; then
					stdin[$id]=
					options+="input "
				fi
				if [[ -v opts[output] ]]; then
					stdout[$id]=
					options+="output "
				fi
				if confirm_request $id && execute_request $id; then
					id_next=$((id+1))
					echo "$owner << accept request $request"
					log "accept request $id {$command} ${options:+with $options}from $owner, execute $id..."
				else
					unset own[$id] cmd[$id] pid[$id] stdin[$id] stdout[$id]
					echo "$owner << reject request $request"
					log "reject request $id {$command} ${options:+with $options}from $owner due to policy/execution"
				fi

			elif [[ -v stdin[$id] ]] && [[ $command =~ $regex_request_input ]]; then
				# ^input (\{(.*)\}|(.+))$
				local input=${BASH_REMATCH[2]:-${BASH_REMATCH[3]}}
				if [[ ${own[$id]} == $owner ]] && [[ -v pid[$id] ]]; then
					echo "$input" >&${stdin[$id]}
					echo "$owner << confirm request $id"
					log "accept request $id input {$input} from $owner, input into $id"
				elif [[ ${own[$id]} != $owner ]]; then
					echo "$owner << reject request $id"
					log "reject request $id input {$input} from $owner since it is owned by ${own[$id]}"
				else
					echo "$owner << reject request $id"
					log "reject request $id input {$input} from $owner since it is not running"
				fi

			elif [[ -v own[$id] ]]; then
				echo "$owner << reject request $id"
				log "reject request $id {$command} from $owner since id $id has been occupied"
			elif [[ $options ]]; then
				echo "$owner << reject request ${id:-{$command\}}"
				log "reject request ${id:+$id }{$command} from $owner due to unsupported option $options"
			else
				echo "$owner << reject request ${id:-{$command\}}"
				local reason="state = ${state[@]:-init}"
				[[ $request_whitelist ]] && reason+=", whitelist = (${broker[@]})"
				log "reject request ${id:+$id }{$command} from $owner, $reason"
			fi

		elif [[ $message =~ $regex_terminate ]]; then
			# ^(\S+) >> terminate (\S+)$
			local who=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[2]}

			if [[ -v pid[$id] ]]; then
				if [ "${own[$id]}" == "$who" ]; then
					echo "$who << accept terminate $id"
					log "accept terminate $id from $who"
					if kill ${pid[$id]} $(cmdpidof $id) 2>/dev/null; then
						log "request $id has been terminated successfully"
					fi
					[[ -v stdin[$id] ]] && exec {stdin[$id]}>&-
					unset own[$id] cmd[$id] res[$id] pid[$id] stdin[$id] stdout[$id]
				else
					echo "$who << reject terminate $id"
					log "reject terminate $id from $who since it is owned by ${own[$id]}"
				fi
			else
				echo "$who << reject terminate $id"
				log "reject terminate $id from $who since no such request"
			fi

		elif [[ $message =~ $regex_others ]]; then
			# ^(\S+) >> (operate|shell|set|unset|use|query|report) (.+)$
			local who=${BASH_REMATCH[1]}
			local command=${BASH_REMATCH[2]}
			local options=${BASH_REMATCH[3]}

			if [ "$command" == "report" ]; then
				if [ "$options" == "state" ]; then
					log "accept report state from $who"
					notify_state $who
				elif [ "$options" == "status" ]; then
					log "accept report status from $who"
					notify_state_with_requests $who
				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -rn1 | sort -n)})
					retain_from ids $(filter_keys own $who)
					erase_from ids ${!pid[@]}
					log "accept report responses from $who, responses = ($(omit ${ids[@]}))"
					for id in ${ids[@]}; do
						echo "$who << response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
				else
					log "ignore $command $options from $who"
				fi &

			elif [ "$command" == "query" ]; then
				if [ "$options" == "protocol" ]; then
					echo "$who << protocol 0 worker 2022-07-30"
					log "accept query protocol from $who"

				elif [ "$options" == "state" ]; then
					echo "$who << state = ${state[@]:-init}"
					log "accept query state from $who, state = ${state[@]:-init}"

				elif [ "$options" == "linked" ]; then
					echo "$who << linked = ${linked[@]}"
					log "accept query linked from $who, linked = ${linked[@]}"

				elif [ "$options" == "capacity" ]; then
					echo "$who << capacity = $((${state[1]#*/}))"
					log "accept query capacity from $who, capacity = $((${state[1]#*/}))"

				elif [ "$options" == "loading" ]; then
					echo "$who << loading = ${state[1]:-0/0}"
					log "accept query loading from $who, loading = ${state[1]:-0/0}"

				elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -rn1 | sort -n)})
					retain_from ids ${!cmd[@]}
					echo "$who << jobs = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v res[$id] ]] && [[ ! -v pid[$id] ]]; then
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}] = ${res[$id]%%:*} {${res[$id]#*:}}"
						else
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}]"
						fi
					done
					log "accept query jobs from $who, jobs = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request|assign)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -rn1 | sort -n)})
					retain_from ids $(filter_keys own $who)
					retain_from ids ${!pid[@]}
					echo "$who << requests = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$who << # request $id {${cmd[$id]}}"
					done
					log "accept query requests from $who, requests = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -rn1 | sort -n)})
					retain_from ids $(filter_keys own $who)
					erase_from ids ${!pid[@]}
					echo "$who << responses = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$who << # response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
					log "accept query responses from $who, responses = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
					local vars=() args=()
					args_of ${BASH_REMATCH[2]:-${set_vars[@]}} >/dev/null
					echo "$who << options = (${vars[@]})"
					[[ ${args[@]} ]] && printf "$who << # %s\n" "${args[@]}"
					log "accept query options from $who, options = ($(omit ${vars[@]}))"

				elif [ "$options" == "envinfo" ]; then
					local envinfo=$(envinfo)
					local envitem=$(cut -d: -f1 <<< "$envinfo" | xargs)
					echo "$who << envinfo = ($envitem)"
					<<< $envinfo xargs -r -d'\n' -L1 echo "$who << #"
					log "accept query envinfo from $who, envinfo = ($envitem)"

				else
					log "ignore $command $options from $who"
				fi &

			elif [ "$command" == "set" ]; then
				local var=(${options/=/ })
				local val=${options:${#var}+1}
				local show_val="$var[@]"
				local val_old="${!show_val}"
				eval $var="\"$val\""
				echo "$who << accept set ${var}${val:+=${val}}"
				log "accept set ${var}${val:+=\"${val}\"} from $who"
				set_vars+=($var)

				if [ "$var" == "broker" ]; then
					change_broker "$val_old" "${broker//:/ }"
				elif [ "$var" == "worker" ]; then
					log "worker name has been changed, register worker on the chat system..."
					echo "name $worker"
				elif [ "$var" == "state" ]; then
					notify_state
				elif [ "$var" == "plugins" ]; then
					xargs_eval -d: source {} >&- 2>&- <<< $plugins
				fi

			elif [ "$command" == "unset" ]; then
				local regex_forbidden_unset="^(worker|state|linked)$"
				local var=(${options/=/ })
				set_vars+=($var)

				if [ "$var" ] && ! [[ $var =~ $regex_forbidden_unset ]]; then
					local show_val="$var[@]"
					local val_old="${!show_val}"
					eval $var=
					echo "$who << accept unset $var"
					log "accept unset $var from $who"

					if [ "$var" == "broker" ]; then
						change_broker "$val_old" ""
					fi

				elif [ "$var" ]; then
					echo "$who << reject unset $var"
				fi

			elif [ "$command" == "operate" ]; then
				if [ "$options" == "shutdown" ]; then
					echo "$who << confirm shutdown"
					log "accept operate shutdown from $who"
					kill ${pid[@]} 2>&-
					return 0

				elif [ "$options" == "restart" ]; then
					echo "$who << confirm restart"
					log "accept operate restart from $who"
					log "${worker:-worker} is restarting..."
					local vars=() args=()
					args_of ${set_vars[@]} >/dev/null
					[[ $tcp_fd ]] && exec 0<&- 1>&-
					kill ${pid[@]} 2>&-
					exec $0 "${args[@]}"

				elif [[ "$options" == "plugin "* ]] || [[ "$options" == "source "* ]]; then
					local mode=${options:0:6}
					local plug=${options:7}
					log "accept operate $mode $plug from $who"
					echo "$who << confirm $mode $plug"
					source $plug >/dev/null 2>&1
					if [[ $mode == "plugin" ]] && [[ :$plugins: != *:$plug:* ]]; then
						plugins+=${plugins:+:}$plug
						log "confirm set plugins=\"$plugins\""
						set_vars+=("plugins")
					fi

				elif [[ "$options" == "output "* ]]; then
					local output=${options:7}
					echo "$output"
					log "accept operate output \"$output\" from $who"
					echo "$who << confirm output $output"

				else
					log "ignore $command $options from $who"
				fi

			elif [ "$command" == "shell" ]; then
				[[ $options =~ ^(\{(.+)\}|(.+))$ ]] && options=${BASH_REMATCH[2]:-${BASH_REMATCH[3]}}
				echo "$who << accept execute shell {$options}"
				log "accept execute shell {$options} from $who"
				{
					local output code lines
					output=$(eval "$options" 2>&1)
					code=$?
					lines=$((${#output} ? $(<<<$output wc -l) : 0))
					echo "$who << result shell {$options} return $code ($lines)"
					echo -n "$output" | xargs -r -d'\n' -L1 echo "$who << #"
				} &

			else
				log "ignore $command $options from $who"
			fi

		elif [[ $message =~ $regex_chat_system ]]; then
			# ^(#|%) (.+)$
			local type=${BASH_REMATCH[1]}
			local info=${BASH_REMATCH[2]}

			if [ "$type" == "#" ]; then
				local regex_login="^login: (\S+)$"
				local regex_logout="^logout: (\S+)$"
				local regex_rename="^name: (\S+) becomes (\S+)$"

				if [[ $info =~ $regex_logout ]]; then
					local who=${BASH_REMATCH[1]}
					if contains own $who && ! [ "${keep_unowned_tasks}" ]; then
						log "$who logged out, discard assignments from $who..."
						discard_owned_assets $(filter_keys own $who)
					fi
					if contains linked $who; then
						log "$who disconnected, wait until $who come back..."
						erase_from linked $who
					fi

				elif [[ $info =~ $regex_rename ]]; then
					local who=${BASH_REMATCH[1]}
					local new=${BASH_REMATCH[2]}

					if contains own $who; then
						log "$who renamed as $new, transfer ownerships..."
						local id
						for id in $(filter_keys own $who); do own[$id]=$new; done
					fi
					if contains linked $who; then
						erase_from broker $who
						erase_from linked $who
						log "$who renamed as $new, make handshake (protocol 0) with $new again..."
						echo "$new << use protocol 0"

					elif contains broker $new; then
						log "$new connected, make handshake (protocol 0) with $new..."
						echo "$new << use protocol 0"
					fi

				elif [[ $info =~ $regex_login ]]; then
					local who=${BASH_REMATCH[1]}

					if [[ " ${broker[@]} " == *" $who "* ]] && ! contains linked $who; then
						log "$who connected, make handshake (protocol 0) with $who..."
						echo "$who << use protocol 0"
					fi
				fi

			elif [ "$type" == "%" ]; then
				if [[ "$info" == "protocol"* ]]; then
					log "chat system protocol verified successfully"
					log "register worker on the chat system..."
					echo "name $worker"
				elif [[ "$info" == "name"* ]]; then
					log "registered as ${worker:=${info:6}} successfully"
					observe_state
					local handshake=(${broker[@]})
					erase_from handshake ${linked[@]}
					if [[ ${handshake[@]} ]]; then
						log "make handshake (protocol 0) with ${handshake[@]}..."
						printf "%s << use protocol 0\n" "${handshake[@]}"
					fi
				elif [[ "$info" == "who: "* ]]; then
					local online=(${info:5})
					if [[ ! $state ]]; then
						if [[ $worker ]]; then
							while contains online $worker; do
								worker=${worker%-*}-$((${worker##*-}+1))
							done
						fi
						log "register worker on the chat system..."
						echo "name $worker"
					else
						local who
						if ! [ "${keep_unowned_tasks}" ] && (( ${#own[@]} )); then
							for who in $(printf "%s\n" "${own[@]}" | sort | uniq); do
								contains online $who && continue
								log "$who logged out silently, discard assignments from $who..."
								discard_owned_assets $(filter_keys own $who)
							done
						fi
						for who in ${broker[@]}; do
							contains online $who && continue
							log "$who disconnected, wait until $who come back..."
							erase_from linked $who
						done
					fi
				elif [[ "$info" == "failed name"* ]]; then
					log "name $worker has been occupied, query online names..."
					echo "who"
				elif [[ "$info" == "failed chat"* ]]; then
					log "failed chat, check online names..."
					echo "who"
				elif [[ "$info" == "failed protocol"* ]]; then
					log "unsupported protocol; shutdown"
					return 1
				fi
			fi

		elif ! [ "$message" ]; then
			jobs >/dev/null 2>&1

		else
			handle_extended_input "$message" || log "ignore message: $message"
		fi

		local id code output
		while (( ${#cmd[@]} )) && fetch_response; do
			if [[ -v cmd[$id] ]] && [[ -v own[$id] ]]; then
				if [ $code != output ]; then
					res[$id]=$code:${stdout[$id]}$output
					[[ -v stdin[$id] ]] && exec {stdin[$id]}>&-
					unset pid[$id]
				else
					stdout[$id]+=$output\\n
				fi
				log "complete response $id $code {$output} and forward it to ${own[$id]}"
				echo "${own[$id]} << response $id $code {$output}"
			else
				log "complete orphan response $id $code {$output}"
			fi
		done

		refresh_state
	done

	log "message input is terminated, chat system is down?"
	return 16
}

confirm_request() {
	local id=$1
	return 0
}

extract_options() {
	opts=()
	local labels="$@" opt val
	for opt in $labels; do
		val="default_$opt"
		[[ ${!val} ]] && opts[$opt]=${!val}
	done
	[[ ! $options ]] && return 0
	options=" $options "
	local regex_option=" (${labels// /|})(=[^ ]*)? "
	while [[ $options =~ $regex_option ]]; do
		options=${options/"$BASH_REMATCH"/ }
		opt=${BASH_REMATCH[1]}
		val=${BASH_REMATCH[2]}
		opts[$opt]=${val:1}
	done
	[[ ! $options ]] && return 0
	options=$(<<< $options xargs)
	[[ ! $options ]]
}

execute_request() {
	local id=$1 output=store
	[[ -v stdout[$id] ]] && output=flush stdout[$id]=
	if [[ ! -v stdin[$id] ]]; then
		execute $id $output >&${res_fd} {res_fd}>&- &
		pid[$id]=$!
		return 0
	elif exec {stdin[$id]}<> <(:); then
		execute $id $output <&${stdin[$id]} >&${res_fd} {stdin[$id]}<&- {res_fd}>&- &
		pid[$id]=$!
		return 0
	else
		return 1
	fi
}

execute() {
	local id=$1 output=${2:-store} code
	if [[ $output == store ]]; then
		output=$(eval "${cmd[$id]}" 2>&1)
		code=$?
		output=$(echo -n "$output" | format_output)
		echo "response $id $code {$output}"
	elif [[ $output == flush ]]; then
		shopt -s lastpipe
		eval "${cmd[$id]}" 2>&1 | while IFS= read -r output; do
			output=$(echo -n "$output" | format_output)
			echo "response $id output {$output}"
		done
		code=${PIPESTATUS[0]}
		if [[ $output ]]; then
			output=$(echo -n "$output" | format_output)
			echo "response $id output {$output}"
		fi
		echo "response $id $code {}"
	else
		echo "response $id -1 {}"
	fi
}

format_output() {
	# drop ASCII terminal color codes then escape '\' '\n' '\t' with '\'
	sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
		sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]'
}

fetch_response() {
	local response
	read -r -t 0 -u ${res_fd} && IFS= read -r -u ${res_fd} response || return $?
	IFS=' ' read -r response id code output <<< $response || return $?
	[ "${output:0:1}${response}${output: -1}" == "{response}" ] || return $?
	output=${output:1:${#output}-2}
	return $?
}

init_system_fd() {
	[[ ${res_fd} ]] || exec {res_fd}<> <(:) && return 0
	log "failed to initialize response pipe"
	return 16
}

discard_owned_assets() {
	local id
	for id in $@; do
		if [[ -v res[$id] ]]; then
			log "discard request $id and response $id"
		elif [[ -v pid[$id] ]]; then
			log "discard and terminate request $id"
			kill ${pid[$id]} $(cmdpidof $id) 2>/dev/null
		fi
		[[ -v stdin[$id] ]] && exec {stdin[$id]}>&-
		unset own[$id] cmd[$id] res[$id] pid[$id] stdin[$id] stdout[$id]
	done
}

change_broker() {
	local current=($1) pending=($2)
	local added=(${pending[@]}) removed=(${current[@]})
	erase_from added ${current[@]}
	erase_from removed ${pending[@]}
	log "confirm broker change: (${current[@]}) --> (${pending[@]})"
	if [[ ${removed[@]} ]] && [[ ${request_whitelist} ]]; then
		log "broker has been changed, discard assignments from ${removed[@]}..."
		local who
		for who in ${removed[@]}; do
			discard_owned_assets $(filter_keys own $who)
		done
	fi
	if [[ ${added[@]} ]]; then
		log "broker has been changed, make handshake (protocol 0) with ${added[@]}..."
		printf "%s << use protocol 0\n" "${added[@]}"
	fi
	erase_from linked ${added[@]} ${removed[@]}
	broker=(${pending[@]})
}

cmdpidof() {
	{ pgrep -P $(pgrep -P ${pid[$1]}); } 2>/dev/null
}

observe_state() {
	local state_last=${state[@]}
	local capacity=$(observe_capacity)
	local stat="idle"
	local num_requests=$((${#cmd[@]} - ${#res[@]}))
	(( ${num_requests} >= ${capacity} )) && stat="busy"
	state=($stat ${num_requests}/${capacity})
	local state_this=${state[@]}
	[ "$state_this" != "$state_last" ]
	return $?
}

observe_capacity() {
	local capacity=${capacity:-$(nproc)}
	if [[ $capacity == *[^0-9]* ]]; then
		capacity=$(bash "$capacity" $worker 2>&-)
		[[ ${capacity:-x} == *[^0-9]* ]] && capacity=0
	fi
	echo $capacity
}

notify_state() {
	local linked=${@:-${linked[@]}}
	[[ $linked ]] || return
	local status="${state[@]:-init}"
	printf "%s << state $status\n" $linked
	log "state $status; notify $linked"
}

notify_state_with_requests() {
	local linked=${@:-${linked[@]}}
	[[ $linked ]] || return
	local status="${state[@]:-init} (${!cmd[@]})"
	printf "%s << state $status\n" $linked
	log "state $status; notify $linked"
}

refresh_state() {
	[[ $state ]] && observe_state && notify_state
}

handle_extended_input() {
	local message=$1
	return 1
}

app() {
	local app=$1
	[[ $app ]] || return 127
	local app_home="${app_home:-.cache/worker}/$app"
	pushd "$app_home" >&- 2>&-
	if [ $? != 0 ]; then # get app package from repo...
		local app_repo=$(realpath -q "${app_repo:-.}" || echo "${app_repo}")
		mkdir -p "$app_home"
		pushd "$app_home" >&- 2>&- || return 128
		local fmt
		for fmt in ".tar.xz" ".tar.gz" ".zip" ""; do
			pkg-get "${app_repo}/${app}${fmt}" && break
		done 2>&-
		if [ $? != 0 ]; then # app package not found
			popd >&- 2>&-
			rm -r "$app_home"
			return -1
		elif [ -e setup.sh ]; then
			bash setup.sh 2>&1 >setup.log
		fi
	fi
	local code
	eval "${@:2}"
	code=$?
	popd >&- 2>&-
	return $code
}

pkg-get() {
	local pkg=$1
	[[ $pkg ]] || return 127
	pkg=$(realpath -q "$pkg" || echo "$pkg")
	local tmp=$(mktemp -d -p .)
	pushd $tmp >/dev/null || return 128
	local code=0
	if [[ $pkg != http* ]]; then
		rsync -aq "$pkg" .
	else
		curl -OJRfs "$pkg"
	fi
	code=$(($?|code))
	popd >/dev/null
	if [ $code == 0 ]; then
		unpack $tmp/*
		code=$(($?|code))
	fi
	rm -r $tmp
	return $code
}

unpack() {
	local code=0 pkg
	for pkg in "$@"; do
		case "${pkg,,}" in
		*.tar.xz|*.txz)	tar Jxf "$pkg"; ;;
		*.tar.gz|*.tgz)	tar zxf "$pkg"; ;;
		*.tar)	tar xf "$pkg"; ;;
		*.xz)	xz -kd "$pkg"; ;;
		*.gz)	gzip -kd "$pkg"; ;;
		*.zip)	unzip -q -o "$pkg"; ;;
		*.7z)	7za x -y "$pkg" >&-; ;;
		esac; code=$(($?|code))
	done
	return $code
}

init_system_io() {
	io_count=${io_count:-0}
	if [[ $1 =~ ^([^:=]+):([0-9]+)$ ]]; then
		local addr=${BASH_REMATCH[1]}
		local port=${BASH_REMATCH[2]}
		while (( $((io_count++)) < ${max_io_count:-65536} )); do
			log "connect to chat system at $addr:$port..."
			if { exec {tcp_fd}<>/dev/tcp/$addr/$port; } 2>/dev/null; then
				log "connected to chat system successfully"
				exec 0<&$tcp_fd 1>&$tcp_fd {tcp_fd}>&- && return 0
			fi
			log "failed to connect $addr:$port, host down?"
			log "wait ${wait_for_conn:-60}s before the next attempt..."
			sleep ${wait_for_conn:-60}
		done
	elif (( $((io_count++)) < ${max_io_count:-1} )); then
		return 0
	fi
	log "max number of connections is reached"
	return 16
}

args_of() {
	args=() vars=()
	local var val arg show=${show:0:3}
	show=${show:-arg}
	for var in "$@"; do
		var=${var%%=*}
		[[ " ${vars[@]} " == *" $var "* ]] && continue
		arg="${var}"
		if [[ $var =~ ^[a-zA-Z_][a-zA-Z_0-9]*$ ]]; then
			val="$var[@]"
			arg+="=${!val}"
		fi
		args+=("$arg")
		vars+=("$var")
		echo "${!show}"
	done
}

omit() {
	if (( "$#" <= ${max_printable_list_size:-10} )); then
		echo "$@"
	else
		local num_show=${num_print_when_omitted:-6}
		echo "${@:1:$((num_show/2))}" "...[$(($#-num_show))]..." "${@:$#-$((num_show/2-1))}"
	fi
}

filter() {
	local item
	for item in "${@:2}"; do [[ $item == $1 ]] && echo "$item"; done
}

filter_keys() {
	local map="$1"
	local key_match="*" val_match="$2"
	[ "$3" ] && local key_match="$2" val_match="$3"
	eval local keys=('${!'$map'[@]}')
	for key in ${keys[@]}; do
		[[ $key == $key_match ]] || continue
		local show=$map[$key]
		local val=$(printf "${vfmt:-%s}" "${!show}")
		[[ $val == $val_match ]] && echo $key
	done
}

contains() {
	local show=${1:-_}[@]
	[[ " ${!show} " == *" ${2} "* ]]
}

erase_from() {
	local -n from=$1
	local patt=" ${@:2} "
	local save item
	for item in ${from[@]}; do
		[[ $patt == *" $item "* ]] || save+="$item "
	done
	eval "${1:-_}=($save)"
}

retain_from() {
	local -n from=$1
	local patt=" ${@:2} "
	local save item
	for item in ${from[@]}; do
		[[ $patt == *" $item "* ]] && save+="$item "
	done
	eval "${1:-_}=($save)"
}

override() {
	eval "$(echo "${1}_default()"; declare -f ${1} | tail -n +2)" 2>&-
}

invoke_overridden() {
	${1}_default "${@:2}"
}

xargs_eval() {
	local item
	local read="read -r"
	case "$1" in
	-d)  read+=" -d\"${2}\""; shift 2; ;;
	-d*) read+=" -d\"${1:2}\""; shift; ;;
	esac
	local exec="${@:-echo}"
	[[ $exec == *"{}"* ]] || exec+=" {}"
	exec=${exec//{\}/\$item}
	while eval $read item; do eval "$exec"; done
	[[ $item ]] && eval "$exec"
}

input() {
	if read -r -t 0; then
		IFS= read -r ${1:-message}
		return $?
	else
		sleep ${system_tick:-0.1}
		eval ${1:-message}=
		return 0
	fi
}

envinfo() {
	# host name
	echo "Host: $(hostname)"
	# OS name and version
	local osinfo=$(uname -o 2>&- | sed "s|GNU/||")
	osinfo+=" $(uname -r | sed -E 's/[^0-9.]+.+$//g')"
	if [[ $OSTYPE =~ cygwin|msys ]]; then
		local ver=($(cmd /c "ver" 2>&- | tr "[\r\n]" " "))
		[[ ${ver[@]} ]] && osinfo+=" (Windows ${ver[-1]})"
	fi
	echo "OS: $osinfo"
	# CPU model
	local cpuinfo=$(grep -m1 "model name" /proc/cpuinfo | sed -E 's/.+:|\(\S+\)|CPU|[0-9]+-Core.+|@.+//g' | xargs)
	local nodes=$(grep "physical id" /proc/cpuinfo | grep -o [0-9] | sort -n | uniq -c)
	local nproc=() num id
	while read -r num id; do nproc[$id]=$num; done <<< $nodes
	if (( ${#nproc[@]} > 1 )) || [[ ! ${nproc[0]} ]]; then
		for id in ${!nproc[@]}; do echo "CPU $id: $cpuinfo (${nproc[$id]}x)"; done
	else
		echo "CPU: $cpuinfo (${nproc:-$(nproc --all)}x)"
	fi
	# CPU affinity
	if [ $(nproc) != $(nproc --all) ]; then
		echo "CPU Affinity: $(taskset -pc $$ | cut -d' ' -f6) ($(nproc)x)"
	fi
	# GPU model
	local nvsmi=$(nvidia-smi -L 2>&- | sed -E "s/ \(UUID:.+$//g" | grep "^GPU")
	[[ $(wc -l <<< $nvsmi) == 1 ]] && nvsmi=${nvsmi/GPU 0:/GPU:}
	xargs -rL1 <<< $nvsmi
	# memory info
	local size=($(head -n1 /proc/meminfo))
	size=$((${size[1]}0/1024/1024))
	size=$((size/10)).$((size%10))
	echo "RAM: $(printf %.1fG $size)"
}

log() {
	local name=$(basename "${0%.sh}")
	logfile=${logfile:-${name}_$(date '+%Y%m%d_%H%M%S_%3N').log}
	if exec 3>> "$logfile" && flock -xn 3; then
		trap 'code=$?; flock -u 3; exit $code' EXIT
		exec 2> >(trap '' INT TERM; exec tee /dev/fd/2 >&3)
	fi
	trap 'log "${'$name:-$name'} has been interrupted"; exit 64' INT
	trap 'log "${'$name:-$name'} has been terminated"; exit 64' TERM
	trap 'code=$?; cleanup 2>&-; log "${'$name:-$name'} is terminated"; exit $code' EXIT

	log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" >&2; }
	log "$@"
}

#### script main routine ####
if [ "$0" == "$BASH_SOURCE" ]; then
	worker_main "$@"
fi
