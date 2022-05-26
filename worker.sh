#!/bin/bash
for var in "$@"; do declare "$var" 2>/dev/null; done

broker=${broker:-broker}
worker=${worker:-worker-1}
max_num_jobs=${max_num_jobs:-$(nproc)}
state_file=${state_file}

stamp=${stamp:-$(date '+%Y%m%d-%H%M%S')}
logfile=${logfile:-$(mktemp --suffix .log $(basename -s .sh "$0")-$stamp.XXXX)}
log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" | tee -a $logfile >&2; }
trap 'cleanup 2>/dev/null; log "${worker:-worker} is terminated";' EXIT

if [[ $1 != NC=* ]]; then
	log "worker version 2022-05-26 (protocol 0)"
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
				$0 NC=$1 "${@:2}" stamp=$stamp logfile=$logfile <&${NC[0]} >&${NC[1]}
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

declare -A own # [id]=requester
declare -A cmd # [id]=command
declare -A pid # [id]=PID
declare state=init # idle|busy

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

observe_state() {
	current_state=$state
	if (( ${#cmd[@]} >= ${max_num_jobs:-1} )); then
		state=busy
	elif [ "$state_file" ] && [ "$(grep -Eo '(idle|busy)' $state_file 2>/dev/null | tail -n1)" == "busy" ]; then
		state=busy
	else
		state=idle
	fi
	[ "$state" != "$current_state" ]
	return $?
}

notify_state() {
	echo "${1:-$broker} << state $state"
	log "state $state; notify ${1:-$broker}"
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

input() {
	unset ${1:-message}
	IFS= read -r -t ${input_timeout:-1} ${1:-message}
	return $(( $? < 128 ? $? : 0 ))
}

regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with ([^{}]*))?|(.+))$"
regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (\S+)$"
regex_terminate="^(\S+) >> terminate (\S+)$"
regex_others="^(\S+) >> (operate|shell|set|unset|use|query) (.+)$"
regex_chat_system="^(#|%) (.+)$"

log "verify chat system protocol 0..."
echo "protocol 0"

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
			log "reject request $id {$command} from $requester due to busy state"
		fi
		[ "$state_file" ] && sleep 0.5
		observe_state; notify_state

	elif [[ $message =~ $regex_confirm_response ]]; then
		who=${BASH_REMATCH[1]}
		confirm=${BASH_REMATCH[2]}
		id=${BASH_REMATCH[3]}
		if [[ -v own[$id] ]]; then
			if [ "${own[$id]}" == "$who" ]; then
				unset own[$id] cmd[$id] pid[$id]
				log "confirm that $who ${confirm}ed response $id"
				[ "$state_file" ] && sleep 0.5
				observe_state; notify_state
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
			state=$option
			log "$broker confirmed state $state"

		elif [ "$who $what" == "$broker protocol" ]; then
			if [ "$confirm" == "accept" ]; then
				log "handshake with $broker successfully"
				observe_state; notify_state

			elif [ "$confirm" == "reject" ]; then
				log "handshake failed, unsupported protocol; exit"
				exit 2
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
				[ "$state_file" ] && sleep 0.5
				observe_state; notify_state
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
				exit 1
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

		regex_set="^set ([^= ]+)([= ].+)?$"
		regex_unset="^unset ([^= ]+)$"
		regex_query_jobs="^query (request|job)s?(.*)$"

		if [[ "$command $options" =~ $regex_set ]]; then
			var=${BASH_REMATCH[1]}
			val=${BASH_REMATCH[2]:1}
			echo "$name << accept set ${var}${val:+ ${val}}"
			declare val_old="${!var}" $var="$val"
			log "accept set ${var}${val:+ as ${val}} from $name"
			if [ "$var" == "broker" ]; then
				log "broker has been changed, make handshake (protocol 0) with $broker again..."
				echo "$broker << use protocol 0"
			elif [ "$var" == "worker" ]; then
				log "worker name has been changed, register $worker on the chat system..."
				echo "name ${worker:=worker-1}"
			elif [ "$var" == "max_num_jobs" ] || [ "$var" == "state_file" ]; then
				observe_state; notify_state
			elif [ "$var" == "state" ]; then
				notify_state
			fi

		elif [[ "$command $options" =~ $regex_unset ]]; then
			var=${BASH_REMATCH[1]}
			regex_forbidden_unset="^(broker|worker|state)$"
			if [ "$var" ] && [[ $var =~ $regex_forbidden_unset ]]; then
				echo "$name << accept unset $var"
				unset $var
				log "accept unset $var from $name"

			elif [ "$var" ]; then
				echo "$name << reject unset $var"
			fi

		elif [ "$command $options" == "query state" ]; then
			log "accept query state from $name"
			notify_state $name

		elif [[ "$command $options" =~ $regex_query_jobs ]] ; then
			ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
			echo "$name << jobs = (${ids[@]})"
			for id in ${ids[@]}; do
				echo "$name << # request $(printf %${#ids[-1]}d $id) ${own[$id]} {${cmd[$id]}}"
			done
			log "accept query jobs from $name"

		elif [ "$command $options" == "operate shutdown" ]; then
			echo "$name << confirm shutdown"
			log "accept operate shutdown from $name"
			exit 0

		elif [ "$command $options" == "operate restart" ]; then
			echo "$name << confirm restart"
			log "accept operate restart from $name"
			log "$worker is restarting..."
			exec $0 $(list_args "$@" broker worker max_num_jobs state_file stamp logfile)

		elif [ "$command $options" == "query envinfo" ]; then
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
exit 16
