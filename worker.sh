#!/bin/bash
cd $(dirname -- "${BASH_SOURCE[0]:-$0}")
for var in "$@"; do declare "$var" 2>/dev/null; done

broker=${broker:-broker}
worker=${worker:-worker-1}
[[ ${capacity-$(nproc)} =~ ^([0-9]+)|(.+)$ ]]
capacity=${BASH_REMATCH[1]}
observe_capacity=${observe_capacity:-${BASH_REMATCH[2]:-capacity.sh}}

session=${session:-$(basename -s .sh "$0")_$(date '+%Y%m%d_%H%M%S')}
logfile=${logfile:-$(mktemp --suffix .log ${session}_XXXX)}
log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" | tee -a $logfile >&2; }
trap 'cleanup 2>/dev/null; log "${worker:-worker} is terminated";' EXIT

if [[ $1 != NC=* ]]; then
	log "worker version 2022-05-28 (protocol 0)"
	log "options: $@"
	bash envinfo.sh 2>/dev/null | while IFS= read -r info; do log "platform $info"; done
	if [[ $1 =~ ^([^:=]+):([0-9]+)$ ]]; then
		addr=${BASH_REMATCH[1]}
		port=${BASH_REMATCH[2]}
		nc=($(command -v ncat nc netcat | xargs -r -L1 basename))
		if ! (( ${#nc[@]} )); then
			log "no available netcat commands (ncat, nc, netcat)"
			exit 16
		fi
		while (( $((conn_count++)) < ${max_conn_count:-65536} )); do
			log "connect to chat system at $addr:$port..."
			coproc NC { $nc $addr $port; }
			sleep ${wait_for_conn:-1}
			if ps -p $NC_PID >/dev/null 2>&1; then
				$0 NC=$1 "${@:2}" session=$session logfile=$logfile <&${NC[0]} >&${NC[1]}
				kill $NC_PID >/dev/null 2>&1
				tail -n2 $logfile | grep -q "shutdown" && exit 0
				code=0; wait_for_conn=1
			else
				log "failed to connect $addr:$port, host down?"
				code=$((code+1)); wait_for_conn=60
			fi
		done
		log "max number of connections is reached"
		exit $code
	fi
elif [[ $1 == NC=* ]]; then
	trap 'cleanup 2>/dev/null;' EXIT
	log "connected to chat system successfully"
fi

run_worker_main() {
	declare -A own # [id]=requester
	declare -A cmd # [id]=command
	declare -A pid # [id]=PID
	declare state=init # (idle|busy #cmd/capacity)

	log "verify chat system protocol 0..."
	echo "protocol 0"

	regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with ([^{}]*))?|(.+))$"
	regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
	regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (.+)$"
	regex_terminate="^(\S+) >> terminate (\S+)$"
	regex_others="^(\S+) >> (operate|shell|set|unset|use|query) (.+)$"
	regex_chat_system="^(#|%) (.+)$"

	while input message; do
		if [[ $message =~ $regex_request ]]; then
			requester=${BASH_REMATCH[1]}
			id=${BASH_REMATCH[4]:-${id_next:-1}}; id_next=$((id+1))
			command=${BASH_REMATCH[5]:-${BASH_REMATCH[8]}}
			options=${BASH_REMATCH[7]}
			observe_state
			if [ "$state" == "idle" ]; then
				if ! [[ -v own[$id] ]]; then
					own[$id]=$requester
					cmd[$id]=$command
					echo "$requester << accept request $id"
					log "accept request $id {$command} from $requester"
					execute $id &
					pid[$id]=$!
				else
					echo "$requester << reject request $id"
					log "reject request $id {$command} from $requester since id $id has been occupied"
				fi
			else
				echo "$requester << reject request $id"
				log "reject request $id {$command} from $requester due to busy state, #cmd = ${#cmd[@]}"
			fi
			observe_state && notify_state

		elif [[ $message =~ $regex_confirm_response ]]; then
			who=${BASH_REMATCH[1]}
			confirm=${BASH_REMATCH[2]}
			id=${BASH_REMATCH[3]}
			if [[ -v own[$id] ]]; then
				if [ "${own[$id]}" == "$who" ]; then
					unset own[$id] cmd[$id] pid[$id]
					log "confirm that $who ${confirm}ed response $id"
					observe_state && notify_state
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

			if [ "$who $confirm $what" == "$broker confirm state" ]; then
				log "$broker confirmed state $option"

			elif [ "$who $what" == "$broker protocol" ]; then
				if [ "$confirm" == "accept" ]; then
					log "handshake with $broker successfully"
					observe_state; notify_state

				elif [ "$confirm" == "reject" ]; then
					log "handshake failed, unsupported protocol; exit"
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
					if kill ${pid[$id]}; then
						log "request $id {${cmd[$id]}} with pid ${pid[$id]} has been terminated successfully"
					fi
					unset own[$id] cmd[$id] pid[$id]
					observe_state && notify_state
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
				if [ "$info" == "logout: $broker" ]; then
					log "$broker disconnected, wait until $broker come back..."
				elif [[ "$info" == "name: $broker becomes "* ]]; then
					broker=${info##* }
					log "broker has been changed, make handshake (protocol 0) with $broker again..."
					echo "$broker << use protocol 0"
				elif [ "$info" == "login: $broker" ] || [[ "$info" == "name: "*" becomes $broker" ]]; then
					log "$broker connected, make handshake (protocol 0) with $broker..."
					echo "$broker << use protocol 0"
				fi
			elif [ "$type" == "%" ]; then
				if [[ "$info" == "protocol"* ]]; then
					log "chat system protocol verified successfully"
					log "register worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [[ "$info" == "failed protocol"* ]]; then
					log "unsupported protocol; exit"
					return 1
				elif [[ "$info" == "name"* ]]; then
					log "registered as $worker successfully"
					log "make handshake (protocol 0) with $broker..."
					echo "$broker << use protocol 0"
				elif [[ "$info" == "failed name"* ]]; then
					occupied=$worker
					worker=${worker%-*}-$((${worker##*-}+1))
					echo "name $worker"
					log "name $occupied has been occupied, try register $worker on the chat system..."
				elif [[ "$info" == "failed chat"* ]]; then
					log "$broker disconnected? wait until $broker come back..."
				fi
			fi

		elif [[ $message =~ $regex_others ]]; then
			name=${BASH_REMATCH[1]}
			command=${BASH_REMATCH[2]}
			options=${BASH_REMATCH[3]}

			if [ "$command" == "query" ]; then
				if [ "$options" == "state" ]; then
					log "accept query state from $name"
					notify_state $name

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
						echo "$name << # request $id {${cmd[$id]}} [${own[$id]}]"
					done
					log "accept query jobs from $name, jobs = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request|assign)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$name" ] && [[ -v cmd[$id] ]] && echo $id; done))
					echo "$name << requests = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$name << # request $id {${cmd[$id]}}"
					done
					log "accept query requests from $name, requests = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
					opts=($(list_args ${BASH_REMATCH[2]:-"$@" ${set_var[@]} session logfile}))
					vars=()
					for opt in ${opts[@]}; do vars+=(${opt%%=*}); done
					echo "$name << options = (${vars[@]})"
					printf "$name << # %s\n" "${opts[@]}"
					log "accept query options from $name, options = ($(list_omit ${vars[@]}))"

				elif [ "$options" == "envinfo" ]; then
					if [ -e envinfo.sh ]; then
						echo "$name << accept query envinfo"
						log "accept query envinfo from $name"
						{
							envinfo=$(bash envinfo.sh 2>/dev/null)
							echo "$name << result envinfo ($(<<<$envinfo wc -l))"
							<<< $envinfo xargs -r -d'\n' -L1 echo "$name << #"
						} &
					else
						echo "$name << reject query envinfo"
						log "reject query envinfo from $name; not installed"
					fi
				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "set" ]; then
				var=(${options/=/ }); var=${var[0]}
				val=${options:$((${#var}+1))}
				set_var+=($var)
				echo "$name << accept set ${var}${val:+ ${val}}"
				declare val_old="${!var}" $var="$val"
				log "accept set ${var}${val:+=\"${val}\"} from $name"
				if [ "$var" == "broker" ]; then
					log "broker has been changed, make handshake (protocol 0) with $broker again..."
					echo "$broker << use protocol 0"
				elif [ "$var" == "worker" ]; then
					log "worker name has been changed, register $worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [ "$var" == "capacity" ] || [ "$var" == "observe_capacity" ]; then
					observe_state && notify_state
				elif [ "$var" == "state" ]; then
					notify_state
				fi

			elif [ "$command" == "unset" ]; then
				var=$options
				set_var+=($var)
				if [ "$var" ] && ! [[ $var =~ ^(broker|worker|state)$ ]]; then
					echo "$name << accept unset $var"
					unset $var
					log "accept unset $var from $name"

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
					exec $0 $(list_args "$@" ${set_var[@]} session logfile)

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
			if [ "$state" != "init" ]; then
				observe_state && notify_state
			fi

		else
			log "ignore message: $message"
		fi
	done

	log "message input is terminated, chat system is down?"
	return 16
}

execute() {
	id=$1
	log "execute request $id {${cmd[$id]}}"
	output=$(eval "${cmd[$id]}" 2>&1)
	code=$?
	# drop ASCII terminal color codes then escape '\' '\n' '\t' with '\'
	output=$(echo -n "$output" | sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
	         sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]')
	log "complete response $id $code {$output} and forward it to ${own[$id]}"
	echo "${own[$id]} << response $id $code {$output}"
}

observe_capacity() {
	if [ -e "$observe_capacity" ]; then
		bash "$observe_capacity" $worker 2>/dev/null && return
		log "failed to observe capacity: $observe_capacity return $?"
	fi
	nproc
}

observe_state() {
	current_state_=${state[@]}
	unset state
	current_capacity=${capacity:-$(observe_capacity)}
	(( ${#cmd[@]} < ${current_capacity:-0} )) && state=idle || state=busy
	state+=(${#cmd[@]}/${current_capacity:-0})
	state_=${state[@]}
	[ "$state_" != "$current_state_" ]
	return $?
}

notify_state() {
	echo "${1:-$broker} << state ${state[@]}"
	log "state ${state[@]}; notify ${1:-$broker}"
}

list_args() {
	declare -A args
	for var in "$@"; do
		var=${var%%=*}
		[[ -v args[$var] ]] || echo $var="${!var}"
		args[$var]=${!var}
	done
	unset args
}

list_omit() {
	if (( "$#" <= ${max_printable_list_size:-10} )); then
		echo "$@"
	else
		num_show=${num_print_when_omitted:-6}
		echo "${@:1:$((num_show/2))}" "...[$(($#-num_show))]..." "${@:$#-$((num_show/2-1))}"
	fi
}

input() {
	unset ${1:-message}
	IFS= read -r -t ${input_timeout:-1} ${1:-message}
	return $(( $? < 128 ? $? : 0 ))
}

run_worker_main
