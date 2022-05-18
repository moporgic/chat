#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'cleanup 2>/dev/null; log "${worker:-worker} is terminated";' EXIT

if [ "$1" != _NC ]; then
	log "worker version 2022-05-18 (protocol 0)"
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
	command=${jobs[$id]}
	log "execute request $id {$command}"
	output=$(eval "$command" 2>&1)
	code=$?
	# drop ASCII terminal color codes then escape '\' '\n' '\t' with '\'
	output="$(<<< $output sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
	          sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]')"
	log "complete response $id $code {$output}; forward to $broker"
	echo "$broker << response $id $code {$output}"
}

log "verify chat system protocol..."
echo "protocol 0"

log "register worker on the chat system..."
echo "name ${worker:=worker-1}"

declare -A jobs # [id]=command
declare -A pids # [id]=PID
declare state=idle

regex_request="^(\S+) >> request (\S+) \{(.+)\}$"
regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (\S+)$"
regex_terminate="^(\S+) >> terminate (\S+)$"
regex_others="^(\S+) >> (operate|set|unset|use|query) (.+)$"
regex_chat_system="^(#|%) (.+)$"

log "start monitoring input..."

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}
		command=${BASH_REMATCH[3]}
		if [ "$requester" == "$broker" ]; then
			if (( ${#jobs[@]} < ${num_jobs:-1} )); then
				jobs[$id]=$command
				echo "$broker << accept request $id"
				log "accept request $id {$command} from $broker"
				execute $id &
				pids[$id]=$!
			else
				echo "$broker << reject request $id"
				log "reject request $id {$command} from $broker since too many running requests"
			fi

			(( ${#jobs[@]} < ${num_jobs:-1} )) && next_state=idle || next_state=busy
			echo "$broker << state $next_state"
			log "state $next_state; notify $broker"
		else
			echo "$requester << reject request $id"
			log "reject request $id {$command} from $requester since it is unauthorized"
		fi

	elif [[ $message =~ $regex_confirm_response ]]; then
		who=${BASH_REMATCH[1]}
		confirm=${BASH_REMATCH[2]}
		id=${BASH_REMATCH[3]}
		if [ "$who" == "$broker" ]; then
			unset jobs[$id] pids[$id]
			log "$broker ${confirm}ed response $id"

			(( ${#jobs[@]} < ${num_jobs:-1} )) && next_state=idle || next_state=busy
			echo "$broker << state $next_state"
			log "state $next_state; notify $broker"
		else
			log "$who ${confirm}ed response $id; ignore since it is unauthorized"
		fi

	elif [[ $message =~ $regex_confirm_others ]]; then
		who=${BASH_REMATCH[1]}
		confirm=${BASH_REMATCH[2]}
		what=${BASH_REMATCH[3]}
		option=${BASH_REMATCH[4]}

		if [ "$who $confirm $what" == "$broker confirm state" ]; then
			if [ "$state" != "$option" ]; then
				state=$option
				log "$broker confirmed state $state"
			fi

		elif [ "$who $what" == "$broker protocol" ]; then
			if [ "$confirm" == "accept" ]; then
				log "handshake with $broker successfully"
				echo "$broker << state ${state:-idle}"
				log "state ${state:-idle}; notify $broker"

			elif [ "$confirm" == "reject" ]; then
				log "handshake failed, unsupported protocol; exit"
				exit 2
			fi

		else
			log "ignore confirmation $confirm $what $option from $who"
		fi

	elif [[ $message =~ $regex_terminate ]]; then
		who=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}

		if [ "$who" == "$broker" ]; then
			if [[ -v pids[$id] ]]; then
				if kill ${pids[$id]}; then
					log "request $id {${jobs[$id]}} with pid ${pids[$id]} has been terminated"
				else
					log "request $id {${jobs[$id]}} with pid ${pids[$id]} may not be terminated"
				fi
				echo "$broker << accept terminate $id"
				unset jobs[$id]
				unset pids[$id]

				(( ${#jobs[@]} < ${num_jobs:-1} )) && next_state=idle || next_state=busy
				echo "$broker << state $next_state"
				log "state $next_state; notify $broker"

			else
				echo "$who << reject terminate $id"
				log "ignore terminate $id from $who since nonexistent request"
			fi
		else
			echo "$who << reject terminate $id"
			log "ignore terminate $id from $who since it is unauthorized"
		fi

	elif [[ $message =~ $regex_chat_system ]]; then
		type=${BASH_REMATCH[1]}
		info=${BASH_REMATCH[2]}

		if [ "$type" == "#" ]; then
			if [ "$info" == "logout: $broker" ]; then
				log "$broker disconnected, wait until $broker come back..."

			elif [[ "$info" == "name: $broker becomes "* ]]; then
				broker=${info##* }
				log "broker has been changed, make handshake with $broker again..."
				echo "$broker << use protocol 0"

			elif [ "$info" == "login: $broker" ] || [[ "$info" == "name: "*" becomes $broker" ]]; then
				log "$broker connected, make handshake with $broker..."
				echo "$broker << use protocol 0"
			fi
		elif [ "$type" == "%" ]; then
			if [[ "$info" == "protocol"* ]]; then
				log "chat system verified protocol 0"
			elif [[ "$info" == "failed protocol"* ]]; then
				log "unsupported protocol; exit"
				exit 1
			elif [[ "$info" == "name"* ]]; then
				log "registered as $broker successfully"
				log "make handshake with $broker..."
				echo "$broker << use protocol 0"
			elif [[ "$info" == "failed name"* ]]; then
				occupied=$worker
				worker=${worker%-*}-$((${worker##*-}+1))
				echo "name $worker"
				log "worker name $occupied is occupied, try register $worker on the chat system..."
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

		if [[ "$command $options" =~ $regex_set ]]; then
			var=${BASH_REMATCH[1]}
			val=${BASH_REMATCH[2]:1}
			echo "$name << accept set ${var}${val:+ ${val}}"
			declare val_old="${!var}" $var="$val"
			log "accept set ${var}${val:+ as ${val}} from $name"
			if [ "$val" != "$val_old" ]; then
				if [ "$var" == "broker" ]; then
					log "broker has been changed, make handshake with $broker again..."
					echo "$broker << use protocol 0"
				elif [ "$var" == "worker" ]; then
					log "worker name has been changed, register $worker on the chat system..."
					echo "name ${worker:=worker-1}"
				elif [ "$var" == "num_jobs" ]; then
					(( ${#jobs[@]} < ${num_jobs:-1} )) && next_state=idle || next_state=busy
					echo "$broker << state $next_state"
					log "state $next_state; notify $broker"
				fi
			fi

		elif [[ "$command $options" =~ $regex_unset ]]; then
			var=${BASH_REMATCH[1]}
			if [ "$var" ] && [ "$var" != "broker" ] && [ "$var" != "worker" ]; then
				echo "$name << accept unset $var"
				unset $var
				log "accept unset $var from $name"

			elif [ "$var" ]; then
				echo "$name << reject unset $var"
			fi

		elif [ "$command $options" == "operate shutdown" ]; then
			echo "$name << confirm shutdown"
			log "accept operate shutdown from $name"
			exit 0

		elif [ "$command $options" == "operate restart" ]; then
			echo "$name << confirm restart"
			log "accept operate restart from $name"
			log "$worker is restarting..."
			>&2 echo
			broker=$broker worker=$worker num_jobs=$num_jobs exec "$0" "$@"

		else
			log "ignore $command $options from $name"
		fi

	else
		log "ignore message: $message"
	fi
done

log "message input is terminated, chat system is down?"
exit 16
