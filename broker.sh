#!/bin/bash
log() { >&2 echo $(date '+%Y-%m-%d %H:%M:%S.%3N') "$@"; }
log "broker version 2022-05-15"

broker=${broker:-broker}

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
log "$broker successfully registered"

declare -A jobs # [id]=requester commands
declare -A assign # [id]=worker
declare -A state # [worker]=idle|busy
queue=()

cleanup() {
	# log "interrupted"
	exit -1
}
trap 'cleanup;' INT

regex_request="^(\S+) >> request (\{(.+)\}|(.+))$"
regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
regex_confirm="^(\S+) >> (accept|reject) (request|response) (\S+)$"
#regex_confirm_request="^(\S+) >> (accept|reject) request (\S+)$"
#regex_confirm_response="^(\S+) >> (accept|reject) response (\S+)$"
regex_worker_state="^(\S+) >> state (idle|busy)$"
#regex_notification="^# (.+)$"
#regex_rename="^# name: (\S+) becomes (\S+)$"
regex_login_logout="^# (login|logout): (\S+)$"
regex_query="^(\S+) >> query (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		commands=${BASH_REMATCH[3]:-${BASH_REMATCH[4]}}
		id=$((++id_counter))
		jobs[$id]="$requester $commands"
		queue+=($id)
		echo "$requester << accept request $id {$commands}"
		log "$requester requests $id {$commands}; accept and enqueue $id; queue = (${queue[@]})"

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
			log "$worker responses $id $code {$output}; accept and forward to $requester"
		else
			echo "$worker << reject response $id"
			if [ "${assign[$id]}" ]; then
				log "$worker responses $id $code {$output}; reject since it is owned by ${assign[$id]}"
			else
				log "$worker responses $id $code {$output}; reject since no such request"
			fi
		fi

	elif [[ $message =~ $regex_worker_state ]]; then
		worker=${BASH_REMATCH[1]}
		state=${BASH_REMATCH[2]}
		echo "$worker << confirm state $state"
		if [ "${state[$worker]}" != "$state" ]; then
			state[$worker]=$state
			log "$worker becomes $state"
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
				log "$name ${confirm}s $type $id; accept"
			elif [ "$confirm" == "reject" ]; then
				if [ "$type" == "request" ]; then
					unset assign[$id]
				fi
				queue=($id ${queue[@]})
				log "$name ${confirm}s $type $id; accept and re-enqueue $id; queue = (${queue[@]})"
			fi
		elif [ "$who" ]; then
			log "$name ${confirm}s $type $id; ignore since it is owned by $who"
		else
			log "$name ${confirm}s $type $id; ignore since no such $type"
		fi

	elif [[ $message =~ $regex_login_logout ]]; then
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

	elif [[ $message =~ $regex_query ]]; then
		name=${BASH_REMATCH[1]}
		query=${BASH_REMATCH[2]}
		if [ "$query" == "protocol" ]; then
			echo "$name << protocol 0"
			log "$name queries protocol; accept"
		else
			log "unknown query: $query"
		fi

	else
		log "ignored message: $message"
	fi

	for worker in ${!state[@]}; do
		if (( ${#queue[@]} )) && [ "${state[$worker]}" == "idle" ]; then
			id=${queue[0]}
			echo "$worker << request $id {${jobs[$id]#* }}"
			assign[$id]=$worker
			unset queue[0]
			log "assign request $id to $worker"
		fi
	done
done
