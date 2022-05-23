#!/bin/bash
for var in "$@"; do declare "$var" 2>/dev/null; done

broker=${broker:-broker}
max_queue_size=${max_queue_size:-65536}

stamp=${stamp:-$(date '+%Y%m%d-%H%M%S')}
logfile=${logfile:-$(mktemp --suffix .log $(basename -s .sh "$0")-$stamp.XXXX)}
log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" | tee -a $logfile >&2; }
trap 'cleanup 2>/dev/null; log "${broker:-broker} is terminated";' EXIT

if [ "$1" != _NC ]; then
	log "broker version 2022-05-23 (protocol 0)"
	bash envinfo.sh 2>/dev/null | while IFS= read -r info; do log "platform $info"; done
	if [[ "$1" =~ ^([^:=]+):([0-9]+)$ ]]; then
		addr=${BASH_REMATCH[1]}
		port=${BASH_REMATCH[2]}
		shift
		log "connect to chat system at $addr:$port..."
		fifo=$(mktemp -u --suffix .fifo .$(basename -s .sh "$0").XXXX)
		mkfifo $fifo
		trap "rm -f $fifo;" EXIT
		nc $addr $port < $fifo | "$0" _NC "$@" stamp=$stamp logfile=$logfile > $fifo && exit 0
		log "unable to connect $addr:$port"
		exit 8
	fi
elif [ "$1" == _NC ]; then
	log "connected to chat system successfully"
	shift
fi

declare -A own # [id]=requester
declare -A cmd # [id]=command
declare -A res # [id]=code:output
declare -A assign # [id]=worker
declare -A state # [worker]=idle|hold|busy
declare -A news # [type-who]=subscribe
declare -A notify # [type]=subscriber...
declare -a queue # id...

input() {
	unset ${1:-message}
	IFS= read -r -t ${input_timeout:-1} ${1:-message}
	return $(( $? < 128 ? $? : 0 ))
}

regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with ([^{}]*))?|(.+))$"
regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
regex_confirm="^(\S+) >> (accept|reject|confirm) (request|response|terminate) (\S+)$"
regex_worker_state="^(\S+) >> state (idle|busy)$"
regex_terminate="^(\S+) >> terminate (\S+)$"
regex_others="^(\S+) >> (query|operate|set|unset|use|subscribe|unsubscribe) (.+)$"
regex_chat_system="^(#|%) (.+)$"
regex_ignore_silently="^(\S+ >> confirm restart)$"

log "verify chat system protocol 0..."
echo "protocol 0"

while input message; do
	if [[ $message =~ $regex_request ]]; then
		requester=${BASH_REMATCH[1]}
		id=${BASH_REMATCH[4]:-$((++id_counter))}
		command=${BASH_REMATCH[5]:-${BASH_REMATCH[8]}}
		options=${BASH_REMATCH[7]}
		if (( ${#queue[@]} < ${max_queue_size:-65536} )); then
			if ! [[ -v own[$id] ]]; then
				own[$id]=$requester
				cmd[$id]=$command
				queue+=($id)
				echo "$requester << accept request $id {$command}"
				log "accept request $id {$command} from $requester and enqueue $id, queue = (${queue[@]})"
			else
				echo "$requester << reject request $id {$command}"
				log "reject request $id {$command} from $requester since id $id has been occupied"
			fi
		else
			echo "$requester << reject request {$command}"
			log "reject request {$command} from $requester due to full queue, queue = (${queue[@]})"
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
				res[$id]=$code:$output
				echo "${own[$id]} << response $id $code {$output}"
				log "accept response $id $code {$output} from $worker and forward it to ${own[$id]}"
			else
				log "accept response $id $code {$output} from $worker but no such request"
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
		log "confirm that $worker state $status"
		if [ "${state[$worker]}" != "$status" ]; then
			state[$worker]=$status
			if (( ${#notify[$status]} )); then
				for subscriber in ${notify[$status]}; do
					echo "$subscriber << notify $worker state $status"
				done
				subscribers=${notify[$status]}
				log "state has been changed, notify ${subscribers// /, }"
			fi
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
				log "confirm that $name ${confirm}ed $type $id"

			elif [ "$confirm" == "reject" ]; then
				if [ "$type" == "request" ]; then
					unset assign[$id]
				fi
				queue=($id ${queue[@]})
				log "confirm that $name ${confirm}ed $type $id and re-enqueue $id, queue = (${queue[@]})"
			fi

		elif [ "$who" ]; then
			log "ignore that $name ${confirm}ed $type $id since it is owned by $who"
		else
			log "ignore that $name ${confirm}ed $type $id since no such $type"
		fi

	elif [[ $message =~ $regex_chat_system ]]; then
		type=${BASH_REMATCH[1]}
		message=${BASH_REMATCH[2]}

		if [ "$type" == "#" ]; then
			names=(${own[@]} ${!state[@]})
			names=${names[@]}
			regex_logout="^logout: (${names// /|})$"
			regex_rename="^name: (${names// /|}) becomes (\S+)$"

			if [[ $message =~ $regex_logout ]]; then
				name=${BASH_REMATCH[1]}
				log "$name logged out"
				if [[ -v state[$name] ]]; then
					for id in ${!assign[@]}; do
						if [ "${assign[$id]}" == "$name" ]; then
							unset assign[$id]
							queue=($id ${queue[@]})
							log "revoke assigned request $id and re-enqueue $id, queue = (${queue[@]})"
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
			elif [[ $message =~ $regex_rename ]]; then
				old_name=${BASH_REMATCH[1]}
				new_name=${BASH_REMATCH[2]}
				log "$old_name renamed as $new_name"
				if [[ -v state[$old_name] ]]; then
					for id in ${!assign[@]}; do
						if [ "${assign[$id]}" == "$old_name" ]; then
							assign[$id]=$new_name
						fi
					done
					state[$new_name]=${state[$old_name]}
					unset state[$old_name]
				fi
				for id in ${!own[@]}; do
					if [ "${own[$id]}" == "$old_name" ]; then
						own[$id]=$new_name
					fi
				done
				for item in ${!notify[@]}; do
					if [[ " ${notify[$item]} " == *" $old_name "* ]]; then
						notify[$item]=$(printf "%s\n" ${notify[$item]} $new_name | sed "/^${old_name}$/d")
						news[$item-$new_name]=${news[$item-$old_name]}
						unset news[$item-$old_name]
					fi
				done
			fi

		elif [ "$type" == "%" ]; then
			if [[ "$message" == "protocol"* ]]; then
				log "chat system protocol verified successfully"
				log "register $broker on the chat system..."
				echo "name $broker"
			elif [[ "$message" == "failed protocol"* ]]; then
				log "unsupported protocol; exit"
				exit 1
			elif [[ "$message" == "name"* ]]; then
				log "registered as $broker successfully"
				if [ "$workers" ]; then
					for worker in ${workers//:/ }; do
						echo "$worker << query state"
					done
					log "query states from ${workers//:/, }..."
					unset workers
				fi
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
				echo "$name << # response $(printf %${#ids[-1]}d $id) ${res[$id]%%:*} {${res[$id]#*:}}"
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
			matches=( $(<<<${BASH_REMATCH[2]:-$broker} grep -Eo '\S+' | while IFS= read -r match; do
				for client in ${!state[@]} $broker; do
					[[ $client == $match ]] && echo $client
				done
			done) )
			declare -A targets
			for match in ${matches[@]}; do targets[$match]=$type; done

			for target in ${!targets[@]}; do
				if [[ -v state[$target] ]]; then
					echo "$name << confirm $type $target"
					log "accept operate $type on $target from $name"
					echo "$target << operate $type"
				fi
			done
			if [[ -v targets[$broker] ]]; then
				echo "$name << confirm $type $broker"
				log "accept operate $type on $broker from $name"
				if [ "$type" == "shutdown" ]; then
					exit 0
				elif [ "$type" == "restart" ]; then
					unset workers
					for worker in ${!state[@]}; do
						if ! [[ -v targets[$worker] ]]; then
							workers+=${workers:+:}$worker
						fi
					done
					log "$broker is restarting..."
					echo >&2
					exec "$0" "$@" broker=$broker max_queue_size=$max_queue_size workers=$workers stamp=$stamp logfile=$logfile
				fi
			fi
			unset targets

		else
			log "ignore $command $options from $name"
		fi

	elif ! [ "$message" ]; then
		:

	elif ! [[ $message =~ $regex_ignore_silently ]]; then
		log "ignore message: $message"
	fi

	for worker in ${!state[@]}; do
		if (( ${#queue[@]} )) && [ "${state[$worker]}" == "idle" ]; then
			id=${queue[0]}
			echo "$worker << request $id {${cmd[$id]}}"
			state[$worker]="hold"
			assign[$id]=$worker
			queue=(${queue[@]:1})

			if [[ -v news[assign-${own[$id]}] ]]; then
				echo "${own[$id]} << notify assign request $id to $worker"
				log "assign request $id to $worker, notify ${own[$id]}"
			else
				log "assign request $id to $worker"
			fi
		fi
	done
done

log "message input is terminated, chat system is down?"
exit 16
