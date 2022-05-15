#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'log "$worker is terminated";' EXIT

broker=${broker:-broker}
worker=${worker:-worker-1}
max_jobs=${max_jobs:-1}

if [ "$1" != "_H" ]; then
	log "worker version 2022-05-15 (protocol 0)"
	if [ "$1" == "-H" ]; then
		addr=${2%:*}
		port=${2#*:}
		shift 2
		log "connect to chat system at $addr:$port..."
		ncat --exec "$0 _H $@" $addr $port && { trap - EXIT; exit 0; }
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
			break
		elif [[ "$reply" == "% failed protocol"* ]]; then
			log "unsupported protocol; exit"
			exit 1
		fi
	done
}
register_worker() {
	log "register worker on the chat system..."
	echo "name ${worker:=worker-1}"
	while IFS= read -r reply; do
		if [ "$reply" == "% name: $worker" ]; then
			break
		elif [[ "$reply" == "% failed name"* ]]; then
			worker=${worker%-*}-$((${worker##*-}+1))
			echo "name $worker"
		fi
	done
	log "registered as $worker successfully"
}
handshake() {
	log "handshake with $broker..."
	echo "$broker << use protocol 0"
	while IFS= read -r reply; do
		regex_failed_chat="^% failed chat.*$"
		if [ "$reply" == "$broker >> accept protocol 0" ]; then
			log "handshake with $broker successfully"
			break
		elif [ "$reply" == "$broker >> reject protocol 0" ]; then
			log "handshake failed, unsupported protocol; exit"
			exit 2
		elif [[ $reply =~ $regex_failed_chat ]]; then
			(( $((wait_count++ % 10)) )) || log "$broker is not connected, wait..."
			sleep 10
			echo "$broker << use protocol 0"
		fi
	done
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
handshake

log "$worker setup completed successfully, start monitoring..."
echo "$broker << state idle"
log "state idle; notify $broker"

declare -A jobs # [id]=command
declare -A pids # [id]=PID
state=none

regex_request="^$broker >> request (\S+) \{(.+)\}$"
regex_confirm_response="^$broker >> (accept|reject) response (\S+)$"
regex_confirm_state="^$broker >> confirm state (idle|busy)$"
regex_notification="^# (.+)$"
regex_others="^(\S+) >> (operate|set|use|query) (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		id=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[2]}
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

	elif [[ $message =~ $regex_confirm_response ]]; then
		confirm=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}
		unset jobs[$id] pids[$id]
		log "$broker ${confirm}ed response $id"

		(( ${#jobs[@]} < $max_jobs )) && next_state=idle || next_state=busy
		echo "$broker << state $next_state"
		log "state $next_state; notify $broker"

	elif [[ $message =~ $regex_confirm_state ]]; then
		if [ "$state" != "${BASH_REMATCH[1]}" ]; then
			state=${BASH_REMATCH[1]}
			log "$broker confirmed state $state"
		fi

	elif [[ $message =~ $regex_notification ]]; then
		info=${BASH_REMATCH[1]}
		if [ "$info" == "logout: $broker" ]; then
			log "$broker disconnected, wait until $broker come back"
			handshake
			(( ${#jobs[@]} < $max_jobs )) && next_state=idle || next_state=busy
			echo "$broker << state $next_state"
			log "state $next_state; notify $broker"
		fi

	elif [[ $message =~ $regex_others ]]; then
		name=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[2]}
		options=${BASH_REMATCH[3]}

		regex_set_broker="^set broker (\S+)$"

		if [[ "$command $options" =~ $regex_set_broker ]]; then
			broker=${BASH_REMATCH[1]}
			echo "$name << confirm set broker $broker"
			log "accept set broker $broker from $name"

		elif [ "$command $options" == "operate shutdown" ]; then
			echo "$name << confirm shutdown"
			log "accept operate shutdown from $name"
			exit 0

		else
			log "unknown $command $options from $name"
		fi

	else
		log "ignored message: $message"
	fi
done

log "message input is terminated, chat system is down?"
