#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
log "worker version 2022-05-15 (protocol 0)"

broker=${broker:-broker}
name=${name:-worker-1}
max_jobs=${max_jobs:-1}

trap 'log "'$name' is terminated";' EXIT

verify_chat_system() {
	log "verify chat system protocol..."
	echo "protocol 0"
	while IFS= read -r reply; do
		if [ "$reply" == "% protocol: 0" ]; then
			log "chat system using protocol 0"
			break
		elif [[ "$reply" == "% failed protocol"* ]]; then
			log "unsupported protocol; exit"
			exit 1
		fi
	done
}
register() {
	log "register myself on the chat system..."
	echo "name ${name:=worker-1}"
	while IFS= read -r reply; do
		if [ "$reply" == "% name: $name" ]; then
			break
		elif [[ "$reply" == "% failed name"* ]]; then
			name=${name%-*}-$((${name##*-}+1))
			echo "name $name"
		fi
	done
	log "registered as $name successfully"
}
handshake() {
	log "handshake with $broker..."
	echo "$broker << query protocol"
	while IFS= read -r reply; do
		regex_protocol="^$broker >> protocol (\S+)$"
		regex_failed_chat="^% failed chat.*$"
		if [[ $reply =~ $regex_protocol ]]; then
			protocol=${BASH_REMATCH[1]}
			if [ "$protocol" == "0" ]; then
				log "handshake with $broker successfully"
				break
			else
				log "handshake failed, unsupported protocol; exit"
				exit 2
			fi
		elif [[ $reply =~ $regex_failed_chat ]]; then
			(( $((wait_count++ % 10)) )) || log "$broker is not connected, wait..."
			sleep 10
			echo "$broker << query protocol"
		fi
	done
}
execute() {
	id=$1
	command=${jobs[$id]}
	log "execute request $id {$command}"
	output=$(eval "$command" 2>&1)
	code=$?
	output="$(<<< $output sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
	          sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]')"
	log "response $id $code {$output}; forward to $broker"
	echo "$broker << response $id $code {$output}"
}

verify_chat_system
register
handshake

log "$name setup completed successfully, start monitoring..."
echo "$broker << state idle"

declare -A jobs # [id]=command
declare -A pids # [id]=PID
state=none

regex_request="^$broker >> request (\S+) \{(.+)\}$"
regex_confirm_response="^$broker >> (accept|reject) response (\S+)$"
regex_confirm_state="^$broker >> confirm state (idle|busy)$"
regex_notification="^# (.+)$"

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

	else
		log "ignored message: $message"
	fi
done

log "message input is terminated, chat system is down?"
