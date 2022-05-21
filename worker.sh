#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'cleanup 2>/dev/null; log "${worker:-worker} is terminated";' EXIT

if [ "$1" != _NC ]; then
	log "worker version 2022-05-21 (protocol 0)"
	bash envinfo.sh 2>/dev/null | while IFS= read -r info; do log "platform $info"; done
	if [[ "$1" =~ ^([^:=]+):([0-9]+)$ ]]; then
		addr=${BASH_REMATCH[1]}
		port=${BASH_REMATCH[2]}
		shift
		log "connect to chat system at $addr:$port..."
		fifo=$(mktemp -u --suffix .fifo $(basename -s .sh "$0").XXXXXXXX)
		mkfifo $fifo
		trap "rm -f $fifo;" EXIT
		nc $addr $port < $fifo | "$0" _NC "$@" > $fifo && exit 0
		log "unable to connect $addr:$port"
		exit 8
	fi
elif [ "$1" == _NC ]; then
	log "connected to chat system successfully"
	shift
fi

broker=${broker:-broker}
worker=${worker:-worker-1}
num_jobs=${num_jobs:-1}
for var in "$@"; do declare "$var"; done

execute() {
	id=$1
	log "execute request $id {${cmd[$id]}}"
	output=$(eval "${cmd[$id]}" 2>&1)
	code=$?
	# drop ASCII terminal color codes then escape '\' '\n' '\t' with '\'
	output="$(<<< $output sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
	          sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]')"
	log "complete response $id $code {$output} and forward it to ${own[$id]}"
	echo "${own[$id]} << response $id $code {$output}"
}

observe_state() {
	(( ${#cmd[@]} < ${num_jobs:-1} )) && state=idle || state=busy
	if [ "$1" == "notify" ]; then
		echo "$broker << state $state"
		log "state $state; notify $broker"
	fi
}

declare -A own # [id]=requester
declare -A cmd # [id]=command
declare -A pid # [id]=PID
declare state=init # idle|busy

regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with ([^{}]*))?|(.+))$"
regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (\S+)$"
regex_terminate="^(\S+) >> terminate (\S+)$"
regex_others="^(\S+) >> (operate|set|unset|use|query) (.+)$"
regex_chat_system="^(#|%) (.+)$"

log "verify chat system protocol 0..."
echo "protocol 0"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[4]:-$((++id_counter))}
		command=${BASH_REMATCH[5]:-${BASH_REMATCH[8]}}
		options=${BASH_REMATCH[7]}
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
		observe_state notify

	elif [[ $message =~ $regex_confirm_response ]]; then
		who=${BASH_REMATCH[1]}
		confirm=${BASH_REMATCH[2]}
		id=${BASH_REMATCH[3]}
		if [[ -v own[$id] ]]; then
			if [ "${own[$id]}" == "$who" ]; then
				unset own[$id] cmd[$id] pid[$id]
				log "confirm that $who ${confirm}ed response $id"
				observe_state notify
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
				observe_state notify

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
				observe_state notify
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
				log "name $occupied is occupied, try register $worker on the chat system..."
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
			if [ "$val" != "$val_old" ]; then
				if [ "$var" == "broker" ]; then
					log "broker has been changed, make handshake (protocol 0) with $broker again..."
					echo "$broker << use protocol 0"
				elif [ "$var" == "worker" ]; then
					log "worker name has been changed, register $worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [ "$var" == "num_jobs" ]; then
					observe_state notify
				elif [ "$var" == "state" ]; then
					log "state $state; notify $broker"
					echo "$broker << state $state"
				fi
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
			observe_state
			echo "$name << state $state"
			log "accept query state from $name"

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
			>&2 echo
			exec "$0" "$@" broker=$broker worker=$worker num_jobs=$num_jobs

		else
			log "ignore $command $options from $name"
		fi

	else
		log "ignore message: $message"
	fi
done

log "message input is terminated, chat system is down?"
exit 16
