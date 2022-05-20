#!/bin/bash
log() { >&2 echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@"; }
trap 'cleanup 2>/dev/null; log "${broker:-broker} is terminated";' EXIT

if [ "$1" != _NC ]; then
	log "broker version 2022-05-19 (protocol 0)"
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
queue_size=${queue_size:-65536}
for var in "$@"; do declare "$var"; done

log "verify chat system protocol..."
echo "protocol 0"

log "register $broker on the chat system..."
echo "name $broker"

declare -A own # [id]=requester
declare -A cmd # [id]=command
declare -A res # [id]=code;output
declare -A assign # [id]=worker
declare -A state # [worker]=idle|hold|busy
declare -A news # [type-who]=subscribe
declare -A notify # [type]=subscriber...
declare -a queue # id...

regex_request="^(\S+) >> request (\{(.+)\}( with ([^{}]*))?|(.+))$"
regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
regex_confirm="^(\S+) >> (accept|reject|confirm) (request|response|terminate) (\S+)$"
regex_worker_state="^(\S+) >> state (idle|busy)$"
regex_terminate="^(\S+) >> terminate (\S+)$"
regex_others="^(\S+) >> (query|operate|set|unset|use|subscribe|unsubscribe) (.+)$"
regex_chat_system="^(#|%) (.+)$"

log "start monitoring input..."

while IFS= read -r message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		command=${BASH_REMATCH[3]:-${BASH_REMATCH[6]}}
		options=${BASH_REMATCH[5]}
		if (( ${#queue[@]} < ${queue_size:-65536} )); then
			id=$((++id_counter))
			own[$id]=$requester
			cmd[$id]=$command
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
			if [[ -v cmd[$id] ]]; then
				res[$id]=$code;$output
				echo "${own[$id]} << response $id $code {$output}"
				log "accept response $id $code {$output} from $worker and forward it to ${own[$id]}"
			else
				log "accept response $id $code {$output} from $worker (no such request)"
			fi
		else
			echo "$worker << reject response $id"
			if [ "${assign[$id]}" ]; then
				log "reject response $id $code {$output} from $worker since it is owned by ${assign[$id]}"
			else
				log "reject response $id $code {$output} from $worker since no such assignment"
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
			who=${own[$id]}
		elif [ "$type" == "request" ] || [ "$type" == "terminate" ]; then
			who=${assign[$id]}
		fi

		if [ "$who" == "$name" ]; then
			if [ "$type" == "terminate" ] && [[ -v own[$id] ]]; then
				echo "${own[$id]} << $confirm terminate $id"
			fi

			if [ "$confirm" == "accept" ] || [ "$confirm" == "confirm" ]; then
				if [ "$type" == "response" ]; then
					unset cmd[$id] own[$id] res[$id]
				elif [ "$type" == "terminate" ]; then
					unset cmd[$id] own[$id] assign[$id] res[$id]
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

	elif [[ $message =~ $regex_chat_system ]]; then
		type=${BASH_REMATCH[1]}
		message=${BASH_REMATCH[2]}

		if [ "$type" == "#" ]; then
			regex_rename="^name: (\S+) becomes (\S+)$"
			regex_login_or_logout="^(login|logout): (\S+)$"

			unset unlink_name
			if [[ $message =~ $regex_rename ]]; then
				old_name=${BASH_REMATCH[1]}
				new_name=${BASH_REMATCH[2]}
				log "$old_name renamed as $new_name"
				unlink_name=$old_name
			fi
			if [[ $message =~ $regex_login_or_logout ]]; then
				type=${BASH_REMATCH[1]}
				name=${BASH_REMATCH[2]}
				if [ "$type" == "logout" ]; then
					log "$name logged out"
					unlink_name=$name
				fi
			fi
			if [ "$unlink_name" ]; then
				name=$unlink_name
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
				for id in ${!own[@]}; do
					if [ "${own[$id]}" == "$name" ]; then
						unset cmd[$id] own[$id]
						if [[ -v res[$id] ]]; then
							unset res[$id]
							log "discard request $id and response $id"
						else
							log "discard request $id"
						fi
						if [[ -v assign[$id] ]]; then
							echo "${assign[$id]} << terminate $id"
							log "terminate assigned request $id on ${assign[$id]}"
						else
							queue=" ${queue[@]} "
							queue=(${queue/ $id / })
						fi
					fi
				done
				for item in ${!notify[@]}; do
					if [[ " ${notify[$item]} " == *" $name "* ]]; then
						notify[$item]=$(printf "%s\n" ${notify[$item]} | sed "/^${name}$/d")
						unset news[$item-$name]
						log "unsubscribe $item for $name"
					fi
				done
			fi

		elif [ "$type" == "%" ]; then
			if [[ "$message" == "protocol"* ]]; then
				log "chat system verified protocol 0"
			elif [[ "$message" == "failed protocol"* ]]; then
				log "unsupported protocol; exit"
				exit 1
			elif [[ "$message" == "name"* ]]; then
				log "registered as $broker successfully"
			elif [[ "$message" == "failed name"* ]]; then
				log "another $broker is already running? exit"
				exit 2
			fi
		fi

	elif [[ $message =~ $regex_terminate ]]; then
		name=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[2]}

		if [[ -v assign[$id] ]]; then
			if [ "$name" == "${own[$id]}" ]; then
				echo "${assign[$id]} << terminate $id"
				log "accept terminate $id from $name and forward it to ${assign[$id]}"
			else
				echo "$name << reject terminate $id"
				log "reject terminate $id from $name since it is owned by ${own[$id]}"
			fi
		elif [[ -v cmd[$id] ]]; then
			queue=" ${queue[@]} "
			queue=(${queue/ $id / })
			unset cmd[$id] own[$id]
			echo "$name << accept terminate $id"
			log "accept terminate $id from $name and remove it from queue"
		else
			echo "$name << reject terminate $id"
			log "reject terminate $id from $name since it is nonexistent"
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
		regex_subscribe="^(subscribe|unsubscribe) (idle|busy|assign)$"
		regex_set="^set ([^= ]+)([= ].+)?$"
		regex_unset="^unset ([^= ]+)$"
		regex_operate_power="^operate (shutdown|restart) ?(.*)$"

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
			ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
			echo "$name << jobs = (${ids[@]})"
			for id in ${ids[@]}; do
				echo "$name << # request $(printf %${#ids[-1]}d $id) ${own[$id]} {${cmd[$id]}}"
			done
			log "accept query jobs from $name"

		elif [[ "$command $options" =~ $regex_query_results ]] ; then
			ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
			echo "$name << results = (${ids[@]})"
			for id in ${ids[@]}; do
				echo "$name << # response $(printf %${#ids[-1]}d $id) ${res[$id]%%;*} {${res[$id]#*;}}"
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
			if [ "$command" == "subscribe" ]; then
				notify[$item]=$(printf "%s\n" ${notify[$item]} $name | sort | uniq)
				news[$item-$name]=subscribe
				echo "$name << accept $command $options"
				log "accept $command $options from $name"
				for worker in ${!state[@]}; do
					if [ "${state[$worker]}" == "$item" ]; then
						echo "$name << notify $worker state $item"
					fi
				done
			elif [ "$command" == "unsubscribe" ]; then
				notify[$item]=$(<<< ${notify[$item]} xargs -r printf "%s\n" | sed "/^${name}$/d")
				unset news[$item-$name]
				echo "$name << accept $command $options"
				log "accept $command $options from $name"
			fi

		elif [[ "$command $options" =~ $regex_set ]]; then
			var=${BASH_REMATCH[1]}
			val=${BASH_REMATCH[2]:1}
			echo "$name << accept set ${var}${val:+ ${val}}"
			declare val_old="${!var}" $var="$val"
			log "accept set ${var}${val:+ as ${val}} from $name"
			if [ "$val" != "$val_old" ]; then
				if [ "$var" == "broker" ]; then
					log "broker name has been changed, register $broker on the chat system..."
					echo "name $broker"
				fi
			fi

		elif [[ "$command $options" =~ $regex_unset ]]; then
			var=${BASH_REMATCH[1]}
			if [ "$var" ] && [ "$var" != "broker" ]; then
				echo "$name << accept unset $var"
				unset $var
				log "accept unset $var from $name"

			elif [ "$var" ]; then
				echo "$name << reject unset $var"
			fi

		elif [[ "$command $options" =~ $regex_operate_power ]]; then
			type=${BASH_REMATCH[1]}
			targets=()
			for match in "${BASH_REMATCH[2]:-$broker}"; do
				for client in ${!state[@]} $broker; do
					if [[ $client == $match ]]; then
						targets+=($client)
					fi
				done
			done

			for target in ${targets[@]}; do
				if [[ -v state[$target] ]]; then
					echo "$name << confirm $type $target"
					log "accept operate $type on $target from $name"
					echo "$target << operate $type"
				fi
			done
			if [[ " ${targets[@]} " == *" $broker "* ]]; then
				echo "$name << confirm $type $broker"
				log "accept operate $type on $broker from $name"
				if [ "$type" == "shutdown" ]; then
					exit 0
				elif [ "$type" == "restart" ]; then
					for worker in ${!state[@]}; do
						echo "$worker << accept protocol 0"
					done
					log "$broker is restarting..."
					>&2 echo
					broker=$broker queue_size=$queue_size exec "$0" "$@"
				fi
			fi

		else
			log "ignore $command $options from $name"
		fi

	else
		log "ignore message: $message"
	fi

	for worker in ${!state[@]}; do
		if (( ${#queue[@]} )) && [ "${state[$worker]}" == "idle" ]; then
			id=${queue[0]}
			echo "$worker << request $id {${cmd[$id]}}"
			state[$worker]="hold"
			assign[$id]=$worker
			queue=(${queue[@]:1})
			log "assign request $id to $worker"
			if [[ -v news[assign-${own[$id]}] ]]; then
				echo "${own[$id]} << notify assign request $id to $worker"
			fi
		fi
	done
done

log "message input is terminated, chat system is down?"
exit 16
