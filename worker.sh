#!/bin/bash

worker_main() {
	declare "$@" >/dev/null 2>&1
	declare set_vars=("$@" broker worker capacity logfile)

	declare broker=${broker-broker}
	declare broker=(${broker//:/ })
	declare worker=${worker:-worker-1}
	declare capacity=${capacity-$(nproc)}
	declare logfile=${logfile}

	log "worker version 2022-06-08 (protocol 0)"
	args_of "${set_vars[@]}" | xargs_ log "option:"
	envinfo | xargs_ log "platform"

	declare -A own # [id]=requester
	declare -A cmd # [id]=command
	declare -A res # [id]=code:output
	declare -A pid # [id]=PID

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
	local state=("init") # (idle|busy #requests/capacity)
	local linked=() # broker...

	log "verify chat system protocol 0..."
	echo "protocol 0"

	local regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with( ([^{}]+)| ?))?|(.+))$"
	local regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
	local regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (.+)$"
	local regex_terminate="^(\S+) >> terminate (\S+)$"
	local regex_others="^(\S+) >> (operate|shell|set|unset|use|query|report) (.+)$"
	local regex_chat_system="^(#|%) (.+)$"

	local message
	while input message; do
		if [[ $message =~ $regex_request ]]; then
			local requester=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[4]}
			local command=${BASH_REMATCH[5]:-${BASH_REMATCH[9]}}
			local options=${BASH_REMATCH[8]}

			if [ "$state" == "idle" ]; then
				if [[ $id ]]; then
					local reply="$id"
				else
					id=${id_next:-1}
					while [[ -v own[$id] ]]; do id=$((id+1)); done
					local reply="$id {$command}"
				fi
				if ! [[ -v own[$id] ]]; then
					own[$id]=$requester
					cmd[$id]=$command
					id_next=$((id+1))
					echo "$requester << accept request $reply"
					log "accept request $id {$command} from $requester"
					log "execute request $id {${cmd[$id]}}..."
					execute $id >&${res_fd} {res_fd}>&- &
					pid[$id]=$!
				else
					echo "$requester << reject request $reply"
					log "reject request $id {$command} from $requester since id $id has been occupied"
				fi
			else
				echo "$requester << reject request ${id:-{$command\}}"
				log "reject request ${id:+$id }{$command} from $requester due to $state state, #cmd = ${#cmd[@]}"
			fi

		elif [[ $message =~ $regex_confirm_response ]]; then
			local who=${BASH_REMATCH[1]}
			local confirm=${BASH_REMATCH[2]}
			local id=${BASH_REMATCH[3]}

			if [[ -v own[$id] ]]; then
				if [ "${own[$id]}" == "$who" ]; then
					log "confirm that $who ${confirm}ed response $id"
					if [ "$confirm" == "accept" ] || [ "$confirm" == "confirm" ]; then
						unset own[$id] cmd[$id] res[$id] pid[$id]
					elif [ "$confirm" == "reject" ]; then
						unset pid[$id] res[$id]
						echo "$who << accept request $id"
						log "execute request $id {${cmd[$id]}}..."
						execute $id >&${res_fd} {res_fd}>&- &
						pid[$id]=$!
					fi
				else
					log "ignore that $who ${confirm}ed response $id since it is owned by ${own[$id]}"
				fi
			else
				log "ignore that $who ${confirm}ed response $id since no such response"
			fi

		elif [[ $message =~ $regex_confirm_others ]]; then
			local who=${BASH_REMATCH[1]}
			local confirm=${BASH_REMATCH[2]}
			local what=${BASH_REMATCH[3]}
			local option=${BASH_REMATCH[4]}

			if [ "$confirm $what" == "confirm state" ]; then
				log "$who confirmed state $option"

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

		elif [[ $message =~ $regex_terminate ]]; then
			local who=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[2]}

			if [[ -v pid[$id] ]]; then
				if [ "${own[$id]}" == "$who" ]; then
					echo "$who << accept terminate $id"
					log "accept terminate $id from $who"
					local cmd_pid=$(pgrep -P $(pgrep -P ${pid[$id]}) 2>/dev/null)
					if [[ $cmd_pid ]] && kill $cmd_pid 2>/dev/null; then
						log "request $id {${cmd[$id]}} with pid $cmd_pid has been terminated successfully"
					else
						log "request $id {${cmd[$id]}} is already terminated"
					fi
					unset own[$id] cmd[$id] res[$id] pid[$id]
				else
					echo "$who << reject terminate $id"
					log "reject terminate $id from $who since it is owned by ${own[$id]}"
				fi
			else
				echo "$who << reject terminate $id"
				log "reject terminate $id from $who since no such request"
			fi

		elif [[ $message =~ $regex_chat_system ]]; then
			local type=${BASH_REMATCH[1]}
			local info=${BASH_REMATCH[2]}

			if [ "$type" == "#" ]; then
				local regex_login="^login: (\S+)$"
				local regex_logout="^logout: (\S+)$"
				local regex_rename="^name: (\S+) becomes (\S+)$"

				if [[ $info =~ $regex_logout ]]; then
					local who=${BASH_REMATCH[1]}
					if contains linked $who; then
						log "$who disconnected, wait until $who come back..."
						erase_from linked $who

					elif contains own $who && ! [ "${keep_unowned_tasks}" ]; then
						log "$who logged out"
						discard_owned_assets $(filter_keys own $who)
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
					fi
					if contains broker $new; then
						log "$new connected, make handshake (protocol 0) with $new..."
						echo "$new << use protocol 0"
					fi

				elif [[ $info =~ $regex_login ]]; then
					local who=${BASH_REMATCH[1]}

					if [[ " ${broker[@]} " == *" $who "* ]]; then
						log "$who connected, make handshake (protocol 0) with $who..."
						echo "$who << use protocol 0"
					fi
				fi

			elif [ "$type" == "%" ]; then
				if [[ "$info" == "protocol"* ]]; then
					log "chat system protocol verified successfully"
					log "register worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [[ "$info" == "failed protocol"* ]]; then
					log "unsupported protocol; shutdown"
					return 1
				elif [[ "$info" == "name"* ]]; then
					log "registered as $worker successfully"
					observe_state
					if [[ ${broker[@]} ]]; then
						log "make handshake (protocol 0) with ${broker[@]}..."
						printf "%s << use protocol 0\n" "${broker[@]}"
					fi
				elif [[ "$info" == "failed name"* ]]; then
					log "name $worker has been occupied, query online names..."
					echo "who"
				elif [[ "$info" == "who: "* ]]; then
					local online=(${info:5})
					if [ "$state" == "init" ]; then
						while contains online $worker; do
							worker=${worker%-*}-$((${worker##*-}+1))
						done
						log "register worker on the chat system..."
						echo "name ${worker:=worker-1}"
					else
						for who in ${broker[@]}; do
							contains online $who && continue
							log "$who disconnected, wait until $who come back..."
							erase_from linked $who
						done
						if ! [ "${keep_unowned_tasks}" ] && (( ${#own[@]} )); then
							local who
							for who in $(printf "%s\n" "${own[@]}" | sort | uniq); do
								contains online $who || contains broker $who && continue
								discard_owned_assets $(filter_keys own $who)
							done
						fi
					fi
				elif [[ "$info" == "failed chat"* ]]; then
					echo "who"
					log "failed chat, check online names..."
				fi
			fi

		elif [[ $message =~ $regex_others ]]; then
			local who=${BASH_REMATCH[1]}
			local command=${BASH_REMATCH[2]}
			local options=${BASH_REMATCH[3]}

			if [ "$command" == "report" ]; then
				if [ "$options" == "state" ]; then
					log "accept report state from $who"
					notify_state $who
				elif [ "$options" == "state with requests" ]; then
					log "accept report state with requests from $who"
					notify_state_with_requests $who
				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$who" ] && [[ -v res[$id] ]] && echo $id; done))
					log "accept report responses from $who, responses = ($(omit ${ids[@]}))"
					for id in ${ids[@]}; do
						echo "$who << response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
				else
					log "ignore $command $options from $who"
				fi

			elif [ "$command" == "query" ]; then
				if [ "$options" == "state" ]; then
					echo "$who << state = ${state[@]}"
					log "accept query state from $who, state = ${state[@]}"

				elif [ "$options" == "linked" ]; then
					echo "$who << linked = ${linked[@]}"
					log "accept query linked from $who, linked = ${linked[@]}"

				elif [ "$options" == "capacity" ]; then
					echo "$who << capacity = ${state[1]#*/}"
					log "accept query capacity from $who, capacity = ${state[1]#*/}"

				elif [ "$options" == "loading" ]; then
					echo "$who << loading = ${state[1]}"
					log "accept query loading from $who, loading = ${state[1]}"

				elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [[ -v cmd[$id] ]] && echo $id; done))
					echo "$who << jobs = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v res[$id] ]]; then
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}] = ${res[$id]%%:*} {${res[$id]#*:}}"
						else
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}]"
						fi
					done
					log "accept query jobs from $who, jobs = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request|assign)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$who" ] && ! [[ -v res[$id] ]] && echo $id; done))
					echo "$who << requests = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$who << # request $id {${cmd[$id]}}"
					done
					log "accept query requests from $who, requests = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$who" ] && [[ -v res[$id] ]] && echo $id; done))
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
					echo "$who << accept query envinfo"
					log "accept query envinfo from $who"
					{
						local envinfo=$(envinfo)
						echo "$who << result envinfo ($(<<<$envinfo wc -l))"
						<<< $envinfo xargs -r -d'\n' -L1 echo "$who << #"
					} &
				else
					log "ignore $command $options from $who"
				fi

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
					log "worker name has been changed, register $worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [ "$var" == "state" ]; then
					notify_state
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
					return 0

				elif [ "$options" == "restart" ]; then
					echo "$who << confirm restart"
					log "accept operate restart from $who"
					log "$worker is restarting..."
					local vars=() args=()
					args_of ${set_vars[@]} >/dev/null
					[[ $tcp_fd ]] && exec 0<&- 1>&-
					exec $0 "${args[@]}"

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

		elif ! [ "$message" ]; then
			:

		else
			log "ignore message: $message"
		fi

		local id code output
		while (( ${#cmd[@]} )) && fetch_response; do
			if [[ -v cmd[$id] ]] && [[ -v own[$id] ]]; then
				res[$id]=$code:$output
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

execute() {
	local id output code
	id=$1
	output=$(eval "${cmd[$id]}" 2>&1)
	code=$?
	# drop ASCII terminal color codes then escape '\' '\n' '\t' with '\'
	output=$(echo -n "$output" | sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
	         sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]')
	echo "response $id $code {$output}"
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
			local cmd_pid=$(pgrep -P $(pgrep -P ${pid[$id]}) 2>/dev/null)
			[[ $cmd_pid ]] && kill $cmd_pid 2>/dev/null
		fi
		unset own[$id] cmd[$id] res[$id] pid[$id]
	done
}

change_broker() {
	local current=($1) pending=($2)
	local added=(${pending[@]}) removed=(${current[@]})
	local who
	for who in ${current[@]}; do erase_from added $who; done
	for who in ${pending[@]}; do erase_from removed $who; done
	log "confirm broker change: (${current[@]}) --> (${pending[@]})"
	if [[ ${removed[@]} ]] && ! [ "${keep_unowned_tasks}" ]; then
		log "broker has been changed, discard assignments of ${removed[@]}..."
		for who in ${removed[@]}; do
			discard_owned_assets $(filter_keys own $who)
		done
	fi
	if [[ ${added[@]} ]]; then
		log "broker has been changed, make handshake (protocol 0) with ${added[@]}..."
		printf "%s << use protocol 0\n" "${added[@]}"
	fi
	for who in ${added[@]} ${removed[@]}; do
		erase_from linked $who
	done
	broker=(${pending[@]})
}

observe_state() {
	local state_last=${state[@]}
	local capacity=${capacity:-$(nproc)}
	if ! [[ $capacity =~ ^[0-9]+$ ]] && [ -e "$capacity" ]; then
		if ! [[ $(bash "$capacity" $worker 2>/dev/null) =~ ^([0-9]+)$ ]]; then
			log "failed to observe capacity with $capacity"
		fi
		capacity=${BASH_REMATCH[1]:-0}
	fi
	local stat="idle"
	local num_requests=$((${#cmd[@]} - ${#res[@]}))
	(( ${num_requests} >= ${capacity} )) && stat="busy"
	state=($stat ${num_requests}/${capacity})
	local state_this=${state[@]}
	[ "$state_this" != "$state_last" ]
	return $?
}

notify_state() {
	local linked=${@:-${linked[@]}}
	[[ $linked ]] || return
	local status=${state[@]}
	printf "%s << state $status\n" $linked
	log "state $status; notify $linked"
}

notify_state_with_requests() {
	local linked=${@:-${linked[@]}}
	[[ $linked ]] || return
	local status="${state[@]} (${!cmd[@]})"
	printf "%s << state $status\n" $linked
	log "state $status; notify $linked"
}

refresh_state() {
	[ "$state" != "init" ] && observe_state && notify_state
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
	local list=${1:-_}[@]
	local value=${2}
	[[ " ${!list} " == *" $value "* ]]
}

erase_from() {
	local list=${1:-_}[@]
	list=" ${!list} "
	eval "${1:-_}=(${list/ ${2} / })"
}

xargs_() {
	local line
	local perform="${@:-echo}"
	[[ $perform == *"{}"* ]] || perform+=" {}"
	perform=${perform//{\}/\$line}
	while IFS= read -r line; do eval "$perform"; done
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

envinfo() (
	exec 2>/dev/null
	# host name
	echo "Host: $(hostname)"
	# OS name and version
	osinfo=$(uname -o 2>/dev/null | sed "s|GNU/||")
	osinfo+=" $(uname -r | sed -E 's/[^0-9.]+.+$//g')"
	if [[ $OSTYPE =~ cygwin|msys ]]; then
		ver=($(cmd /c ver 2>/dev/null | tr "[\r\n]" " "))
		(( ${#ver[@]} )) && osinfo+=" (Windows ${ver[-1]})"
	fi
	echo "OS: $osinfo"
	# CPU model
	cpuinfo=$(grep -m1 name /proc/cpuinfo | sed -E 's/.+:|\(\S+\)|CPU|[0-9]+-Core.+|@.+//g' | xargs)
	nodes=$(lscpu | grep 'NUMA node(s)' | cut -d: -f2 | xargs)
	if (( ${nodes:-1} != 1 )); then
		for (( i=0; i<${nodes:-1}; i++ )); do
			echo "CPU $i: $cpuinfo ($(taskset -c $(lscpu | grep 'NUMA node'$i' CPU' | cut -d: -f2) nproc)x)"
		done
	else
		echo "CPU: $cpuinfo ($(nproc --all)x)"
	fi
	# CPU affinity
	if [ "$(nproc)" != "$(nproc --all)" ]; then
		echo "CPU $(taskset -pc $$ | cut -d' ' -f4-) ($(nproc)x)"
	fi
	# GPU model
	nvidia-smi -L 2>/dev/null | sed -E "s/ \(UUID:.+$//g" | while IFS= read GPU; do echo "$GPU"; done
	# memory info
	size=($(head -n1 /proc/meminfo))
	size=$((${size[1]}0/1024/1024))
	size=$((size/10)).$((size%10))
	echo "RAM: $(printf "%.1fG" $size)"
)

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
