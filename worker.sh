#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'log "$worker is terminated";' EXIT

broker=${broker:-broker}
worker=${worker:-worker-1}
max_jobs=${max_jobs:-1}


if [ "$1" != "_H" ]; then
	log "worker version 2022-05-16 (protocol 0)"
	if [ "$1" == "-H" ]; then
		addr=${2%:*}
		port=${2#*:}
		shift 2
		log "connect to chat system at $addr:$port..."
		trap - EXIT
		ncat --exec "$0 _H $@" $addr $port && exit 0
		log "unable to connect $addr:$port"
		exit 8
	fi
else
	log "connected to chat system successfully"
	shift
fi

verify_chat_system() {
	log "verify chat system protocol..."
	echo "protocol 0"
	while IFS= read -r reply; do
		if [ "$reply" == "% protocol: 0" ]; then
			log "chat system verified protocol 0"
			return
		elif [[ "$reply" == "% failed protocol"* ]]; then
			log "unsupported protocol; exit"
			exit 1
		fi
	done
	log "failed to verify protocol; exit"
	exit 1
}
register_worker() {
	log "register worker on the chat system..."
	echo "name ${worker:=worker-1}"
	while IFS= read -r reply; do
		if [ "$reply" == "% name: $worker" ]; then
			log "registered as $worker successfully"
			return
		elif [[ "$reply" == "% failed name"* ]]; then
			worker=${worker%-*}-$((${worker##*-}+1))
			echo "name $worker"
		fi
	done
	log "failed to register worker; exit"
	exit 2
}
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

verify_chat_system
register_worker

log "$worker setup completed successfully, make handshake with $broker..."
echo "$broker << use protocol 0"

declare -A jobs # [id]=command
declare -A pids # [id]=PID
state=idle

regex_request="^(\S+) >> request (\S+) \{(.+)\}$"
regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
regex_confirm_others="^(\S+) >> (confirm|accept|reject) (state|protocol) (\S+)$"
regex_failed_chat="^% failed chat.*$"
regex_notification="^# (.+)$"
regex_others="^(\S+) >> (operate|set|use|query) (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}
		command=${BASH_REMATCH[3]}
		if [ "$requester" == "$broker" ]; then
			if (( ${#jobs[@]} < $max_jobs )); then
				jobs[$id]=$command
				echo "$broker << accept request $id"
				log "accept request $id {$command} from $broker"
				execute $id &
				pids[$id]=$!
			else
				echo "$broker << reject request $id"
				log "reject request $id {$command} from $broker since too many running requests"
			fi

			(( ${#jobs[@]} < $max_jobs )) && next_state=idle || next_state=busy
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

			(( ${#jobs[@]} < $max_jobs )) && next_state=idle || next_state=busy
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

	elif [[ $message =~ $regex_notification ]]; then
		info=${BASH_REMATCH[1]}

		if [ "$info" == "logout: $broker" ]; then
			log "$broker disconnected, wait until $broker come back..."

		elif [ "$info" == "login: $broker" ] || [[ "$info" == "name: "*" becomes $broker" ]]; then
			log "$broker connected, make handshake with $broker..."
			echo "$broker << use protocol 0"
		fi

	elif [[ $message =~ $regex_failed_chat ]]; then
		log "$broker disconnected, wait until $broker come back..."

	elif [[ $message =~ $regex_others ]]; then
		name=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[2]}
		options=${BASH_REMATCH[3]}

		regex_set_broker="^set broker (\S+)$"

		if [[ "$command $options" =~ $regex_set_broker ]]; then
			broker=${BASH_REMATCH[1]}
			echo "$name << confirm set broker $broker"
			log "accept set broker $broker from $name, make handshake with $broker..."
			echo "$broker << use protocol 0"

		elif [ "$command $options" == "operate shutdown" ]; then
			echo "$name << confirm shutdown"
			log "accept operate shutdown from $name"
			exit 0

		else
			log "ignore $command $options from $name"
		fi

	else
		log "ignore message: $message"
	fi
done

log "message input is terminated, chat system is down?"
