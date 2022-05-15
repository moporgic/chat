#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
log "broker version 2022-05-15"

broker=${broker:-broker}
max_queued_jobs=${max_queued_jobs:-65536}

trap 'log "'$broker' is terminated";' EXIT

log "check chat system protocol..."
echo "protocol 0"
while IFS= read -r reply; do
	if [ "$reply" == "% protocol: 0" ]; then
		log "chat system using protocol 0"
		break
	elif [[ "$reply" == "% failed protocol"* ]]; then
		log "protocol mismatched!"
		exit 1
	fi
done
log "register $broker on the chat system..."
echo "name $broker"
while IFS= read -r reply; do
	if [ "$reply" == "% name: $broker" ]; then
		break
	elif [[ "$reply" == "% failed name"* ]]; then
		log "another $broker is already running?"
		exit 2
	fi
done

log "$broker setup completed successfully, start monitoring..."

declare -A jobs # [id]=requester commands
declare -A assign # [id]=worker
declare -A state # [worker]=idle|hold|busy
queue=()

regex_request="^(\S+) >> request (\{(.+)\}|(.+))$"
regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
regex_confirm="^(\S+) >> (accept|reject) (request|response) (\S+)$"
regex_worker_state="^(\S+) >> state (idle|busy)$"
regex_notification="^# (.+)$"
regex_rename="^# name: (\S+) becomes (\S+)$"
regex_login_logout="^# (login|logout): (\S+)$"
regex_query="^(\S+) >> query (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		commands=${BASH_REMATCH[3]:-${BASH_REMATCH[4]}}
		if (( ${#queue[@]} < ${max_queued_jobs:-65536} )); then
			id=$((++id_counter))
			jobs[$id]="$requester $commands"
			queue+=($id)
			echo "$requester << accept request $id {$commands}"
			log "accept request $id {$commands} from $requester and enqueue $id; queue = (${queue[@]})"
		else
			echo "$requester << reject request {$commands}"
			log "reject request $id {$commands} from $requester since too many queued requests; queue = (${queue[@]})"
		fi

	elif [[ $message =~ $regex_response ]]; then
		worker=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}
		code=${BASH_REMATCH[3]}
		output=${BASH_REMATCH[4]}

		if [ "${assign[$id]}" == "$worker" ]; then
			unset assign[$id]
			echo "$worker << accept response $id"
			requester=${jobs[$id]%% *}
			echo "$requester << response $id $code {$output}"
			log "accept response $id $code {$output} from $worker and forward it to $requester"
		else
			echo "$worker << reject response $id"
			if [ "${assign[$id]}" ]; then
				log "reject response $id $code {$output} from $worker since it is owned by ${assign[$id]}"
			else
				log "reject response $id $code {$output} from $worker since no such request"
			fi
		fi

	elif [[ $message =~ $regex_worker_state ]]; then
		worker=${BASH_REMATCH[1]}
		status=${BASH_REMATCH[2]}
		echo "$worker << confirm state $status"
		if [ "${state[$worker]}" != "$status" ]; then
			state[$worker]=$status
			log "confirm $worker state $status"
		fi

	elif [[ $message =~ $regex_confirm ]]; then
		name=${BASH_REMATCH[1]}
		confirm=${BASH_REMATCH[2]}
		type=${BASH_REMATCH[3]}
		id=${BASH_REMATCH[4]}

		if [ "$type" == "response" ]; then
			who=${jobs[$id]%% *}
		elif [ "$type" == "request" ]; then
			who=${assign[$id]}
		fi

		if [ "$who" == "$name" ]; then
			if [ "$confirm" == "accept" ]; then
				if [ "$type" == "response" ]; then
					unset jobs[$id]
				fi
				log "$name ${confirm}ed $type $id; confirm"
			elif [ "$confirm" == "reject" ]; then
				if [ "$type" == "request" ]; then
					unset assign[$id]
				fi
				queue=($id ${queue[@]})
				log "$name ${confirm}ed $type $id; confirm and re-enqueue $id; queue = (${queue[@]})"
			fi
		elif [ "$who" ]; then
			log "$name ${confirm}ed $type $id; ignore since it is owned by $who"
		else
			log "$name ${confirm}ed $type $id; ignore since no such $type"
		fi

	elif [[ $message =~ $regex_notification ]]; then
		if [[ $message =~ $regex_login_logout ]]; then
			type=${BASH_REMATCH[1]}
			name=${BASH_REMATCH[2]}
			if [ "$type" == "logout" ] && [[ -v state[$name] ]]; then
				log "$name logged out"
				for id in ${!assign[@]}; do
					if [ "${assign[$id]}" == "$name" ]; then
						unset assign[$id]
						queue=($id ${queue[@]})
						log "revoke assigned request $id; re-enqueue $id; queue = (${queue[@]})"
					fi
				done
				unset state[$name]
			fi
		fi

	elif [[ $message =~ $regex_query ]]; then
		name=${BASH_REMATCH[1]}
		query=${BASH_REMATCH[2]}
		if [ "$query" == "protocol" ]; then
			echo "$name << protocol 0"
			log "accept query protocol from $name"
		else
			log "unknown query $query from $name"
		fi

	else
		log "ignored message: $message"
	fi

	for worker in ${!state[@]}; do
		if (( ${#queue[@]} )) && [ "${state[$worker]}" == "idle" ]; then
			id=${queue[0]}
			echo "$worker << request $id {${jobs[$id]#* }}"
			state[$worker]="hold"
			assign[$id]=$worker
			queue=(${queue[@]:1})
			log "assign request $id to $worker"
		fi
	done
done
