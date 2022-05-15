#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'log "$broker is terminated";' EXIT

broker=${broker:-broker}
max_queued_jobs=${max_queued_jobs:-65536}

if [ "$1" != "_H" ]; then
	log "broker version 2022-05-16 (protocol 0)"
	if [ "$1" == "-H" ]; then
		addr=${2%:*}
		port=${2#*:}
		shift 2
		log "connect to chat system at $addr:$port..."
		fifo=$(mktemp -u /tmp/broker.XXXXXXXX)
		mkfifo $fifo
		trap "rm -f $fifo;" EXIT
		${nc:-nc} $addr $port < $fifo | "$0" _H "$@" > $fifo && exit 0
		log "unable to connect $addr:$port"
		exit 8
	fi
elif [ "$1" == "_H" ]; then
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
register_broker() {
	log "register $broker on the chat system..."
	echo "name $broker"
	while IFS= read -r reply; do
		if [ "$reply" == "% name: $broker" ]; then
			log "registered as $broker successfully"
			return
		elif [[ "$reply" == "% failed name"* ]]; then
			log "another $broker is already running? exit"
			exit 2
		fi
	done
	log "failed to register $broker; exit"
	exit 2
}

verify_chat_system
register_broker

log "$broker setup completed successfully, start monitoring..."

declare -A jobs # [id]=requester command
declare -A results # [id]=code output
declare -A assign # [id]=worker
declare -A state # [worker]=idle|hold|busy
declare -A news # [type-who]=subscribe
declare -A notify # [type]=subscriber...
queue=()

regex_request="^(\S+) >> request (\{(.+)\}|(.+))$"
regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
regex_confirm="^(\S+) >> (accept|reject) (request|response) (\S+)$"
regex_worker_state="^(\S+) >> state (idle|busy)$"
regex_notification="^# (.+)$"
regex_rename="^# name: (\S+) becomes (\S+)$"
regex_login_or_logout="^# (login|logout): (\S+)$"
regex_others="^(\S+) >> (operate|set|use|subscribe|query) (.+)$"

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[3]:-${BASH_REMATCH[4]}}
		if (( ${#queue[@]} < ${max_queued_jobs:-65536} )); then
			id=$((++id_counter))
			jobs[$id]="$requester $command"
			queue+=($id)
			echo "$requester << accept request $id {$command}"
			log "accept request $id {$command} from $requester and enqueue $id, queue = (${queue[@]})"
		else
			echo "$requester << reject request {$command}"
			log "reject request $id {$command} from $requester since too many queued requests, queue = (${queue[@]})"
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
			for subscriber in ${notify[$status]}; do
				echo "$subscriber << notify $worker state $status"
			done
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
				log "$name ${confirm}ed $type $id; confirm and re-enqueue $id, queue = (${queue[@]})"
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
			if [ "$type" == "logout" ]; then
				log "$name logged out"
				if [[ -v state[$name] ]]; then
					for id in ${!assign[@]}; do
						if [ "${assign[$id]}" == "$name" ]; then
							unset assign[$id]
							queue=($id ${queue[@]})
							log "revoke assigned request $id; re-enqueue $id, queue = (${queue[@]})"
						fi
					done
					unset state[$name]
				fi
				for item in ${!notify[@]}; do
					if [[ " ${notify[$item]} " == *" $name "* ]]; then
						notify[$item]=$(printf "%s\n" ${notify[$item]} | sed "/^${name}$/d")
						unset news[$item-$name]
						log "cancel subscribed $item of $name"
					fi
				done
			fi
		fi

	elif [[ $message =~ $regex_others ]]; then
		name=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[2]}
		options=${BASH_REMATCH[3]}

		regex_use_protocol="^use protocol (\S+)$"
		regex_query_jobs="^query (request|job)s?(.*)$"
		regex_query_results="^query (response|result)s?(.*)$"
		regex_query_assign="^query (assign(ment)?|task)s?$"
		regex_query_state="^query (state|worker)s?$"
		regex_subscribe="^subscribe (cancel )?(idle|busy|assign)$"

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

		elif [[ "$command $options" =~ $regex_query_assign ]]; then
			assignment=()
			for id in ${!assign[@]}; do
				assignment+=("[$id]=${assign[$id]}")
			done
			echo "$name << assign = (${assignment[@]})"
			log "accept query assign from $name"

		elif [[ "$command $options" =~ $regex_query_state ]]; then
			status=()
			for worker in ${!state[@]}; do
				status+=("[$worker]=${state[$worker]}")
			done
			echo "$name << state = (${status[@]})"
			log "accept query state from $name"

		elif [[ "$command $options" =~ $regex_subscribe ]]; then
			item=$options
			if ! [[ "$item" == "cancel "* ]]; then
				notify[$item]=$(printf "%s\n" ${notify[$item]} $name | sort | uniq)
				news[$item-$name]=subscribe
				echo "$name << accept $command $options"
				log "accept $command $options from $name"
				for worker in ${!state[@]}; do
					if [ "${state[$worker]}" == "$item" ]; then
						echo "$name << notify $worker state $item"
					fi
				done
			else
				item=${item#* }
				notify[$item]=$(printf "%s\n" ${notify[$item]} | sed "/^${name}$/d")
				unset news[$item-$name]
				echo "$name << accept $command $options"
				log "accept $command $options from $name"
			fi

		elif [ "$command $options" == "operate shutdown" ]; then
			echo "$name << confirm shutdown"
			log "accept operate shutdown from $name"
			exit 0

		elif [ "$command $options" == "operate restart" ]; then
			echo "$name << confirm restart"
			log "accept operate restart from $name"
			log "$broker is restarting..."
			log ""
			echo "name ${broker}_$$__"
			broker=$broker max_queued_jobs=$max_queued_jobs exec "$0" "$@"

		else
			log "ignore $command $options from $name"
		fi

	else
		log "ignore message: $message"
	fi

	for worker in ${!state[@]}; do
		if (( ${#queue[@]} )) && [ "${state[$worker]}" == "idle" ]; then
			id=${queue[0]}
			echo "$worker << request $id {${jobs[$id]#* }}"
			state[$worker]="hold"
			assign[$id]=$worker
			queue=(${queue[@]:1})
			log "assign request $id to $worker"
			requester="${jobs[$id]%% *}"
			if [[ -v news[assign-$requester] ]]; then
				echo "$requester << notify assign request $id to $worker"
			fi
		fi
	done
done

log "message input is terminated, chat system is down?"
