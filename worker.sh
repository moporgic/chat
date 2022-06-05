#!/bin/bash

worker_main() {
	log "worker version 2022-06-05 (protocol 0)"
	for var in "$@"; do declare "$var" 2>/dev/null; done

	broker=${broker-broker}; broker=(${broker//:/ })
	worker=${worker:-worker-1}
	[[ ${capacity-$(nproc)} =~ ^([0-9]+)|(.+)$ ]] && capacity=${BASH_REMATCH[1]}
	observe_capacity=${observe_capacity:-${BASH_REMATCH[2]:-capacity.sh}}

	declare -A own # [id]=requester
	declare -A cmd # [id]=command
	declare -A res # [id]=code:output
	declare -A pid # [id]=PID
	declare -a linked # broker...
	declare state # (idle|busy #cmd/capacity)

	list_args "$@" $(common_vars) | while IFS= read -r opt; do log "option: $opt"; done
	list_envinfo | while IFS= read -r info; do log "platform $info"; done

	trap 'log "${worker:-worker} has been interrupted"; exit 64' INT
	trap 'log "${worker:-worker} has been terminated"; exit 64' TERM

	while init_system_io "$@" && init_system_fd "$@"; do
		worker_routine "$@"
		local code=$?
		(( $code < 16 )) && break
	done

	log "${worker:-worker} is terminated"
	return $code
}

worker_routine() {
	state=init
	linked=()

	log "verify chat system protocol 0..."
	echo "protocol 0"

	local regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with( ([^{}]+)| ?))?|(.+))$"
	local regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
	local regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (.+)$"
	local regex_terminate="^(\S+) >> terminate (\S+)$"
	local regex_others="^(\S+) >> (operate|shell|set|unset|use|query|report) (.+)$"
	local regex_chat_system="^(#|%) (.+)$"

	while input message; do
		if [[ $message =~ $regex_request ]]; then
			requester=${BASH_REMATCH[1]}
			id=${BASH_REMATCH[4]}
			command=${BASH_REMATCH[5]:-${BASH_REMATCH[9]}}
			options=${BASH_REMATCH[8]}

			if [ "$state" == "idle" ]; then
				if [[ $id ]]; then
					reply="$id"
				else
					id=${id_next:-1}
					while [[ -v own[$id] ]]; do id=$((id+1)); done
					reply="$id {$command}"
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
				refresh_state
			else
				echo "$requester << reject request ${id:-\{$command\}}"
				log "reject request ${id:+$id }{$command} from $requester due to $state state, #cmd = ${#cmd[@]}"
			fi

		elif [[ $message =~ $regex_confirm_response ]]; then
			who=${BASH_REMATCH[1]}
			confirm=${BASH_REMATCH[2]}
			id=${BASH_REMATCH[3]}

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
					refresh_state
				else
					log "ignore that $who ${confirm}ed response $id since it is owned by ${own[$id]}"
				fi
			else
				log "ignore that $who ${confirm}ed response $id since no such response"
			fi

		elif [[ $message =~ $regex_confirm_others ]]; then
			who=${BASH_REMATCH[1]}
			confirm=${BASH_REMATCH[2]}
			what=${BASH_REMATCH[3]}
			option=${BASH_REMATCH[4]}

			if [ "$confirm $what" == "confirm state" ]; then
				log "$who confirmed state $option"

			elif [ "$what" == "protocol" ]; then
				if [ "$confirm" == "accept" ]; then
					log "handshake with $who successfully"
					linked=($(erase_from linked $who) $who)
					observe_state; notify_state $who

				elif [ "$confirm" == "reject" ]; then
					log "handshake failed, unsupported protocol; shutdown"
					return 2
				fi

			else
				log "ignore that $who $confirm $what $option from $who"
			fi

		elif [[ $message =~ $regex_terminate ]]; then
			who=${BASH_REMATCH[1]}
			id=${BASH_REMATCH[2]}

			if [[ -v pid[$id] ]]; then
				if [ "${own[$id]}" == "$who" ]; then
					echo "$who << accept terminate $id"
					log "accept terminate $id from $who"
					cmd_pid=$(pgrep -P $(pgrep -P ${pid[$id]}) 2>/dev/null)
					if [[ $cmd_pid ]] && kill $cmd_pid 2>/dev/null; then
						log "request $id {${cmd[$id]}} with pid $cmd_pid has been terminated successfully"
					else
						log "request $id {${cmd[$id]}} is already terminated"
					fi
					unset own[$id] cmd[$id] res[$id] pid[$id]
					refresh_state
				else
					echo "$who << reject terminate $id"
					log "reject terminate $id from $who since it is owned by ${own[$id]}"
				fi
			else
				echo "$who << reject terminate $id"
				log "reject terminate $id from $who since no such request"
			fi

		elif [[ $message =~ $regex_chat_system ]]; then
			type=${BASH_REMATCH[1]}
			info=${BASH_REMATCH[2]}

			if [ "$type" == "#" ]; then
				regex_login="^login: (\S+)$"
				regex_logout="^logout: (\S+)$"
				regex_rename="^name: (\S+) becomes (\S+)$"

				if [[ $info =~ $regex_logout ]]; then
					name=${BASH_REMATCH[1]}
					if [[ " ${linked[@]} " == *" $name "* ]]; then
						log "$name disconnected, wait until $name come back..."
						linked=($(erase_from linked $name))

					elif [[ " ${own[@]} " == *" $name "* ]] && ! [ "${keep_unowned_tasks}" ]; then
						log "$name logged out"
						for id in ${!own[@]}; do
							[ "${own[$id]}" == "$name" ] && discard_owned_asset $id
						done
					fi

				elif [[ $info =~ $regex_rename ]]; then
					old_name=${BASH_REMATCH[1]}
					new_name=${BASH_REMATCH[2]}

					if [[ " ${own[@]} " == *" $old_name "* ]]; then
						log "$old_name renamed as $new_name, transfer ownerships..."
						for id in ${!own[@]}; do
							[ "${own[$id]}" == "$old_name" ] && own[$id]=$new_name
						done
					fi
					if [[ " ${linked[@]} " == *" $old_name "* ]]; then
						broker=($(erase_from broker $old_name))
						linked=($(erase_from linked $old_name))
						log "$old_name renamed as $new_name, make handshake (protocol 0) with $new_name again..."
						echo "$new_name << use protocol 0"
					fi
					if [[ " ${broker[@]} " == *" $new_name "* ]]; then
						log "$new_name connected, make handshake (protocol 0) with $new_name..."
						echo "$new_name << use protocol 0"
					fi

				elif [[ $info =~ $regex_login ]]; then
					name=${BASH_REMATCH[1]}

					if [[ " ${broker[@]} " == *" $name "* ]]; then
						log "$name connected, make handshake (protocol 0) with $name..."
						echo "$name << use protocol 0"
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
					if [ "$state" == "init" ]; then
						while [[ " ${info:5} " == *" $worker "* ]]; do
							worker=${worker%-*}-$((${worker##*-}+1))
						done
						log "register worker on the chat system..."
						echo "name ${worker:=worker-1}"
					else
						for name in ${broker[@]}; do
							[[ " ${info:5} " == *" $name "* ]] && continue
							log "$name disconnected, wait until $name come back..."
							linked=($(erase_from linked $name))
						done
						if ! [ "${keep_unowned_tasks}" ]; then
							for name in $(printf "%s\n" "${own[@]}" | sort | uniq); do
								[[ " ${info:5} " == *" $name "* ]] && continue
								[[ " ${broker[@]} " == *" $name "* ]] && continue
								for id in ${!own[@]}; do
									[ "${own[$id]}" == "$name" ] && discard_owned_asset $id
								done
							done
						fi
					fi
				elif [[ "$info" == "failed chat"* ]]; then
					echo "who"
					log "failed chat, check online clients..."
				fi
			fi

		elif [[ $message =~ $regex_others ]]; then
			name=${BASH_REMATCH[1]}
			command=${BASH_REMATCH[2]}
			options=${BASH_REMATCH[3]}

			if [ "$command" == "report" ]; then
				if [ "$options" == "state" ]; then
					log "accept report state from $name"
					notify_state $name
				elif [ "$options" == "state with requests" ]; then
					log "accept report state with requests from $name"
					notify_state_with_requests $name
				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$name" ] && [[ -v res[$id] ]] && echo $id; done))
					log "accept report responses from $name, responses = ($(list_omit ${ids[@]}))"
					for id in ${ids[@]}; do
						echo "$name << response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "query" ]; then
				if [ "$options" == "state" ]; then
					echo "$name << state = ${state[@]}"
					log "accept query state from $name, state = ${state[@]}"

				elif [ "$options" == "linked" ]; then
					echo "$name << linked = ${linked[@]}"
					log "accept query linked from $name, linked = ${linked[@]}"

				elif [ "$options" == "capacity" ]; then
					current_capacity=${capacity:-$(observe_capacity)}
					echo "$name << capacity = ${current_capacity:-0}"
					log "accept query capacity from $name, capacity = ${current_capacity:-0}"

				elif [ "$options" == "loading" ]; then
					current_capacity=${capacity:-$(observe_capacity)}
					echo "$name << loading = ${#cmd[@]}/${current_capacity:-0}"
					log "accept query loading from $name, loading = ${#cmd[@]}/${current_capacity:-0}"

				elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [[ -v cmd[$id] ]] && echo $id; done))
					echo "$name << jobs = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v res[$id] ]]; then
							echo "$name << # $id {${cmd[$id]}} [${own[$id]}] = ${res[$id]%%:*} {${res[$id]#*:}}"
						else
							echo "$name << # $id {${cmd[$id]}} [${own[$id]}]"
						fi
					done
					log "accept query jobs from $name, jobs = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request|assign)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$name" ] && ! [[ -v res[$id] ]] && echo $id; done))
					echo "$name << requests = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$name << # request $id {${cmd[$id]}}"
					done
					log "accept query requests from $name, requests = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$name" ] && [[ -v res[$id] ]] && echo $id; done))
					echo "$name << responses = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$name << # response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
					log "accept query responses from $name, responses = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
					list_args ${BASH_REMATCH[2]:-"$@" $(common_vars) ${set_var[@]}} >/dev/null
					echo "$name << options = (${vars[@]})"
					printf "$name << # %s\n" "${args[@]}"
					log "accept query options from $name, options = ($(list_omit ${vars[@]}))"

				elif [ "$options" == "envinfo" ]; then
					echo "$name << accept query envinfo"
					log "accept query envinfo from $name"
					{
						envinfo=$(list_envinfo)
						echo "$name << result envinfo ($(<<<$envinfo wc -l))"
						<<< $envinfo xargs -r -d'\n' -L1 echo "$name << #"
					} &
				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "set" ]; then
				var=(${options/=/ })
				val=${options:${#var}+1}
				set_var+=($var)
				local show_val="$var[@]"
				eval val_old="${!show_val}"
				eval $var="$val"
				echo "$name << accept set ${var}${val:+=${val}}"
				log "accept set ${var}${val:+=\"${val}\"} from $name"

				if [ "$var" == "broker" ]; then
					change_broker "$val_old" "${broker//:/ }"
				elif [ "$var" == "worker" ]; then
					log "worker name has been changed, register $worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [ "$var" == "capacity" ] || [ "$var" == "observe_capacity" ]; then
					[[ ${capacity-$(nproc)} =~ ^([0-9]+)|(.+)$ ]] && capacity=${BASH_REMATCH[1]}
					observe_capacity=${observe_capacity:-${BASH_REMATCH[2]:-capacity.sh}}
					refresh_state
				elif [ "$var" == "state" ]; then
					notify_state
				fi

			elif [ "$command" == "unset" ]; then
				var=(${options/=/ })
				set_var+=($var)
				regex_forbidden_unset="^(worker|state|linked)$"

				if [ "$var" ] && ! [[ $var =~ $regex_forbidden_unset ]]; then
					local show_val="$var[@]"
					declare val_old="${!show_val}"
					unset $var
					echo "$name << accept unset $var"
					log "accept unset $var from $name"

					if [ "$var" == "broker" ]; then
						change_broker "$val_old" ""
					fi

				elif [ "$var" ]; then
					echo "$name << reject unset $var"
				fi

			elif [ "$command" == "operate" ]; then
				if [ "$options" == "shutdown" ]; then
					echo "$name << confirm shutdown"
					log "accept operate shutdown from $name"
					return 0

				elif [ "$options" == "restart" ]; then
					echo "$name << confirm restart"
					log "accept operate restart from $name"
					log "$worker is restarting..."
					list_args "$@" $(common_vars) ${set_var[@]} >/dev/null
					exec $0 "${args[@]}"

				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "shell" ]; then
				[[ $options =~ ^(\{(.+)\}|(.+))$ ]] && options=${BASH_REMATCH[2]:-${BASH_REMATCH[3]}}
				echo "$name << accept execute shell {$options}"
				log "accept execute shell {$options} from $name"
				{
					output=$(eval "$options" 2>&1)
					code=$?
					lines=$((${#output} ? $(<<<$output wc -l) : 0))
					echo "$name << result shell {$options} return $code ($lines)"
					echo -n "$output" | xargs -r -d'\n' -L1 echo "$name << #"
				} &

			else
				log "ignore $command $options from $name"
			fi

		elif ! [ "$message" ]; then
			refresh_state

		else
			log "ignore message: $message"
		fi

		while (( ${#cmd[@]} )) && fetch_response; do
			if [[ -v cmd[$id] ]] && [[ -v own[$id] ]]; then
				res[$id]=$code:$output
				log "complete response $id $code {$output} and forward it to ${own[$id]}"
				echo "${own[$id]} << response $id $code {$output}"
			else
				log "complete orphan response $id $code {$output}"
			fi
		done
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

discard_owned_asset() {
	local id=${1:?id} cmd_pid
	if [[ -v res[$id] ]]; then
		log "discard request $id and response $id"
	elif [[ -v pid[$id] ]]; then
		log "discard and terminate request $id"
		cmd_pid=$(pgrep -P $(pgrep -P ${pid[$id]}) 2>/dev/null)
		[[ $cmd_pid ]] && kill $cmd_pid 2>/dev/null
	fi
	unset own[$id] cmd[$id] res[$id] pid[$id]
}

change_broker() {
	local current=($1) pending=($2)
	local added=(${pending[@]}) removed=(${current[@]})
	local who id
	for who in ${current[@]}; do added=($(erase_from added $who)); done
	for who in ${pending[@]}; do removed=($(erase_from removed $who)); done
	log "$name << confirm broker change: (${current[@]}) --> (${pending[@]})"
	for id in ${!own[@]}; do
		[[ " ${removed[@]} " == *" ${own[$id]} "* ]] && discard_owned_asset $id
	done
	for who in ${added[@]} ${removed[@]}; do
		linked=($(erase_from linked $who))
	done
	if [[ ${added[@]} ]]; then
		log "broker has been changed, make handshake (protocol 0) with ${added[@]}..."
		printf "%s << use protocol 0\n" "${added[@]}"
	fi
	broker=(${pending[@]})
}

observe_capacity() {
	if [ -e "$observe_capacity" ]; then
		bash "$observe_capacity" $worker 2>/dev/null && return
		log "failed to observe capacity: $observe_capacity return $?"
	fi
	nproc
}

observe_state() {
	local current_state_=${state[@]}
	local current_capacity=${capacity:-$(observe_capacity)}
	state=()
	(( ${#cmd[@]} < ${current_capacity:-0} )) && state=idle || state=busy
	state+=(${#cmd[@]}/${current_capacity:-0})
	local state_=${state[@]}
	[ "$state_" != "$current_state_" ]
	return $?
}

notify_state() {
	local who
	for who in ${@:-${linked[@]}}; do
		echo "$who << state ${state[@]}"
		log "state ${state[@]}; notify $who"
	done
}

notify_state_with_requests() {
	local who
	for who in ${@:-${linked[@]}}; do
		echo "$who << state ${state[@]} (${!cmd[@]})"
		log "state ${state[@]} (${!cmd[@]}); notify $who"
	done
}

refresh_state() {
	[ "$state" != "init" ] && observe_state && notify_state
}

common_vars() {
	echo broker worker capacity observe_capacity logfile
}

init_system_io() {
	conn_count=${conn_count:-0}
	if [[ $1 =~ ^([^:=]+):([0-9]+)$ ]]; then
		local addr=${BASH_REMATCH[1]}
		local port=${BASH_REMATCH[2]}
		local wait_for_conn=0
		while (( $((conn_count++)) < ${max_conn_count:-65536} )); do
			log "connect to chat system at $addr:$port..."
			sleep ${wait_for_conn:-0}
			if { exec {nc}<>/dev/tcp/$addr/$port; } 2>/dev/null; then
				log "connected to chat system successfully"
				exec 0<&$nc 1>&$nc {nc}<&- {nc}>&- && return 0
			fi
			log "failed to connect $addr:$port, host down?"
			wait_for_conn=60
		done
	elif (( $((conn_count++)) < ${max_conn_count:-1} )); then
		return 0
	fi
	log "max number of connections is reached"
	return 16
}

list_args() {
	args=()
	vars=()
	local var val arg
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
	done
	printf "%s\n" "${args[@]}"
}

list_omit() {
	if (( "$#" <= ${max_printable_list_size:-10} )); then
		echo "$@"
	else
		local num_show=${num_print_when_omitted:-6}
		echo "${@:1:$((num_show/2))}" "...[$(($#-num_show))]..." "${@:$#-$((num_show/2-1))}"
	fi
}

erase_from() {
	local list=${1:?}[@]
	local value=${2:?}
	list=" ${!list} "
	echo ${list/ $value / }
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

list_envinfo() (
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

init_logfile() {
	for var in "$@"; do declare "$var" 2>/dev/null; done
	declare -g session=${session:-$(basename -s .sh "$0")_$(date '+%Y%m%d_%H%M%S')}
	declare -g logfile=${logfile:-$(mktemp --suffix .log ${session}_XXXX)}
	exec 3>> $logfile
	if flock -xn 3; then
		trap 'code=$?; flock -u 3; exit $code' EXIT
		exec 2> >(trap '' INT TERM; tee /dev/fd/2 >&3)
	fi
}

log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" >&2; }

#### script main routine ####
init_logfile "$@"
worker_main "$@"
