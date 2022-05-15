#!/bin/bash
log() { >&2 echo $(date '+%Y-%m-%d %H:%M:%S.%3N') "$@"; }
log "worker version 2022-05-15"

broker=${broker:-broker}
name=${name:-worker-1}
max_jobs=${max_jobs:-1}
heartbeat=${heartbeat:-60}

log "check chat system protocol..."
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
log "check $broker protocol..."
echo "$broker << query protocol"
while IFS= read -r reply; do
	regex_protocol="^$broker >> protocol (\S+)$"
	regex_failed_chat="^% failed chat.*$"
	if [[ $reply =~ $regex_protocol ]]; then
		protocol=${BASH_REMATCH[1]}
		log "$broker using protocol $protocol"
		if [ "$protocol" == "0" ]; then
			break
		else
			log "unsupported protocol; exit"
			exit 2
		fi
	elif [[ $reply =~ $regex_failed_chat ]]; then
		log "$broker is not connected..."
		sleep 10
		echo "$broker << query protocol"
	fi
done
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

[ -e $name.run ] && mv -f $name.run $name.run.$(date '+%Y-%m-%d-%H-%M-%S' -r $name.run)
mkdir $name.run || { log "failed to setup output folder"; exit 3; }
echo $$ > $name.run/pid

declare -A jobs # [id]=commands
declare -A pids # [id]=PID
state=none

notify_state() {
	while true; do
		echo "$broker << state ${1:-idle}"
		sleep ${heartbeat:-60}
	done
}
become_idle() {
	log "become idle; start notification"
	kill ${pids[state]} 2>/dev/null
	notify_state idle &
	pids[state]=$!
}
become_busy() {
	log "become busy; start notification"
	kill ${pids[state]} 2>/dev/null
	notify_state busy &
	pids[state]=$!
}
norm_output() {
	sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g'
}
execute() {
	id=$1
	commands=${jobs[$id]}
	echo "$1 {$commands}" >> $name.run/request.txt
	log "execute request $id {$commands}"
	eval "$commands" > $name.run/$id.output 2>&1
	code=$?
	output="$(norm_output < $name.run/$id.output)"
	log "response $id $code {$output}; forward to $broker"
	echo "$broker << response $id $code {$output}"
}
cleanup() {
	# log "interrupted"
	kill ${pids[state]} 2>/dev/null
	unset pids[state]
	for id in ${!pids[@]}; do
		kill ${pids[$id]} 2>/dev/null && log "request $id {${jobs[$id]}} has been terminated"
	done
	rm -f $name.run/pid 2>/dev/null
	exit -1
}
trap 'cleanup;' INT

become_idle

regex_request="^$broker >> request (\S+) \{(.+)\}$"
regex_confirm_response="^$broker >> (accept|reject) response (\S+)$"
regex_confirm_state="^$broker >> confirm state (idle|busy)$"
regex_notification="^# (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		id=${BASH_REMATCH[1]}
		commands=${BASH_REMATCH[2]}
		if (( ${#jobs[@]} < $max_jobs )); then
			jobs[$id]=$commands
			echo "$broker << accept request $id"
			log "accept request $id {$commands}"
			if (( ${#jobs[@]} >= $max_jobs )); then
				become_busy
			fi
			execute $id &
			pids[$id]=$!
		else
			echo "$broker << reject request $id"
			log "reject request $id; too many requests"
		fi

	elif [[ $message =~ $regex_confirm_response ]]; then
		confirm=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}
		unset jobs[$id] pids[$id]
		log "$broker ${confirm}ed response $id"
		if (( ${#jobs[@]} < $max_jobs )); then
			become_idle
		fi

	elif [[ $message =~ $regex_confirm_state ]]; then
		if [ "$state" != "${BASH_REMATCH[1]}" ]; then
			state=${BASH_REMATCH[1]}
			log "$broker confirmed state $state"
		fi

	elif [[ $message =~ $regex_notification ]]; then
		info=${BASH_REMATCH[1]}
		if [ "info" == "logout: $broker" ]; then
			log "$broker disconnected"
		fi

	else
		log "ignored message: $message"
	fi
done
