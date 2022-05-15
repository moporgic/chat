#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'log "$broker is terminated";' EXIT

broker=${broker:-broker}
max_queued_jobs=${max_queued_jobs:-65536}

if [ "$1" != "_H" ]; then
	log "broker version 2022-05-15 (protocol 0)"
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
register_broker() {
	log "register $broker on the chat system..."
	echo "name $broker"
	while IFS= read -r reply; do
		if [ "$reply" == "% name: $broker" ]; then
			break
		elif [[ "$reply" == "% failed name"* ]]; then
			log "another $broker is already running? exit"
			exit 2
		fi
	done
	log "registered as $broker successfully"
}

verify_chat_system
register_broker

log "$broker setup completed successfully, start monitoring..."

declare -A jobs # [id]=requester command
declare -A results # [id]=code output
declare -A assign # [id]=worker
declare -A state # [worker]=idle|hold|busy
queue=()

regex_request="^(\S+) >> request (\{(.+)\}|(.+))$"
regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
regex_confirm="^(\S+) >> (accept|reject) (request|response) (\S+)$"
regex_worker_state="^(\S+) >> state (idle|busy)$"
regex_notification="^# (.+)$"
regex_rename="^# name: (\S+) becomes (\S+)$"
regex_login_or_logout="^# (login|logout): (\S+)$"
regex_others="^(\S+) >> (operate|set|use|query) (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[3]:-${BASH_REMATCH[4]}}
		if (( ${#queue[@]} < ${max_queued_jobs:-65536} )); then
			id=$((++id_counter))
			jobs[$id]="$requester $command"
			queue+=($id)
			echo "$requester << accept request $id {$command}"
			log "accept request $id {$command} from $requester and enqueue $id; queue = (${queue[@]})"
		else
			echo "$requester << reject request {$command}"
			log "reject request $id {$command} from $requester since too many queued requests; queue = (${queue[@]})"
		fi

	elif [[ $message =~ $regex_response ]]; then
		worker=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}
		code=${BASH_REMATCH[3]}
		output=${BASH_REMATCH[4]}

		if [ "${assign[$id]}" == "$worker" ]; then
			unset assign[$id]
			echo "$worker << accept response $id"
			requester="${jobs[$id]%% *}"
			results[$id]="$code $output"
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
					unset results[$id]
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
		if [[ $message =~ $regex_login_or_logout ]]; then
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

	elif [[ $message =~ $regex_others ]]; then
		name=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[2]}
		options=${BASH_REMATCH[3]}

		regex_use_protocol="^use protocol (\S+)$"
		regex_query_jobs="^query (request|job)s?(.*)$"
		regex_query_results="^query (response|result)s?(.*)$"

		if [ "$command $options" == "query protocol" ]; then
			echo "$name << protocol 0"
			log "accept query protocol from $name"

		elif [[ "$command $options" =~ $regex_use_protocol ]]; then
			protocol=${BASH_REMATCH[1]}
			if [ "$protocol" == "0" ]; then
				echo "$name << accept protocol $protocol"
				log "accept use protocol $protocol from $name"
			else
				echo "$name << reject protocol $protocol"
				log "reject use protocol $protocol from $name, unsupported protocol"
			fi

		elif [ "$command $options" == "query queue" ]; then
			echo "$name << queue = (${queue[@]})"
			log "accept query queue from $name"

		elif [[ "$command $options" =~ $regex_query_jobs ]] ; then
			ids=(${BASH_REMATCH[2]:-$(printf "%d\n" ${!jobs[@]} | sort -n)})
			echo "$name << jobs = (${ids[@]})"
			for id in ${ids[@]}; do
				requester="${jobs[$id]%% *}"
				options="${jobs[$id]#* }"
				echo "$name << # request $(printf %${#ids[-1]}d $id) $requester {$options}"
			done
			log "accept query jobs from $name"

		elif [[ "$command $options" =~ $regex_query_results ]] ; then
			ids=(${BASH_REMATCH[2]:-$(printf "%d\n" ${!results[@]} | sort -n)})
			echo "$name << results = (${ids[@]})"
			for id in ${ids[@]}; do
				code="${results[$id]%% *}"
				output="${results[$id]#* }"
				echo "$name << # response $(printf %${#ids[-1]}d $id) $code {$output}"
			done
			log "accept query results from $name"

		elif [ "$command $options" == "query assign" ]; then
			assignment=()
			for id in ${!assign[@]}; do
				assignment+=("[$id]=${assign[$id]}")
			done
			echo "$name << assign = (${assignment[@]})"
			log "accept query assign from $name"

		elif [ "$command $options" == "query state" ]; then
			status=()
			for worker in ${!state[@]}; do
				status+=("[$worker]=${state[$worker]}")
			done
			echo "$name << state = (${status[@]})"
			log "accept query state from $name"

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

log "message input is terminated, chat system is down?"
