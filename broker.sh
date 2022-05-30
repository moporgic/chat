#!/bin/bash
cd $(dirname -- "${BASH_SOURCE[0]:-$0}")
for var in "$@"; do declare "$var" 2>/dev/null; done

broker=${broker:-broker}
capacity=${capacity:-65536}
default_timeout=${default_timeout:-0}
default_workers=${default_workers}

session=${session:-$(basename -s .sh "$0")_$(date '+%Y%m%d_%H%M%S')}
logfile=${logfile:-$(mktemp --suffix .log ${session}_XXXX)}
log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" | tee -a $logfile >&2; }

startup_broker() {
	log "broker version 2022-05-30 (protocol 0)"
	list_args $(common_vars) "$@" | while IFS= read -r opt; do log "option: $opt"; done
	list_envinfo | while IFS= read -r info; do log "platform $info"; done
	if [[ $1 =~ ^([^:=]+):([0-9]+)$ ]]; then
		addr=${BASH_REMATCH[1]}
		port=${BASH_REMATCH[2]}
		nc=($(command -v ncat nc netcat | xargs -r -L1 basename))
		if ! (( ${#nc[@]} )); then
			log "no available netcat commands (ncat, nc, netcat)"
			exit 16
		fi
		while (( $((conn_count++)) < ${max_conn_count:-65536} )); do
			log "connect to chat system at $addr:$port..."
			coproc NC { $nc $addr $port; }
			sleep ${wait_for_conn:-1}
			if ps -p $NC_PID >/dev/null 2>&1; then
				log "connected to chat system successfully"
				$0 NC=$1 "${@:2}" session=$session logfile=$logfile <&${NC[0]} >&${NC[1]}
				kill $NC_PID >/dev/null 2>&1
				tail -n2 $logfile | grep -q "shutdown" && exit 0
				code=0; wait_for_conn=1
			else
				log "failed to connect $addr:$port, host down?"
				code=$((code+1)); wait_for_conn=60
			fi
		done
		log "max number of connections is reached"
		exit $code
	fi
}

run_broker_main() {
	declare -A own # [id]=requester
	declare -A cmd # [id]=command
	declare -A res # [id]=code:output
	declare -A assign # [id]=worker
	declare -A prefer # [id]=worker
	declare -A tmout # [id]=timeout
	declare -A tmdue # [id]=due
	declare -A state # [worker]=stat:load
	declare -A news # [type-who]=subscribe
	declare -A notify # [type]=subscriber...
	declare -a queue # id...

	log "verify chat system protocol 0..."
	echo "protocol 0"

	regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with ([^{}]*))?|(.+))$"
	regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
	regex_confirm="^(\S+) >> (accept|reject|confirm) (request|response|terminate) (\S+)$"
	regex_worker_state="^(\S+) >> state (idle|busy) (\S+/\S+)$"
	regex_others="^(\S+) >> (query|terminate|operate|shell|set|unset|use|subscribe|unsubscribe) (.+)$"
	regex_chat_system="^(#|%) (.+)$"

	while input message; do
		if [[ $message =~ $regex_request ]]; then
			requester=${BASH_REMATCH[1]}
			id=${BASH_REMATCH[4]:-${id_next:-1}}; id_next=$((id+1))
			command=${BASH_REMATCH[5]:-${BASH_REMATCH[8]}}
			options=${BASH_REMATCH[7]}
			if (( ${#cmd[@]} < ${capacity:-65536} )); then
				if ! [[ -v own[$id] ]]; then
					own[$id]=$requester
					cmd[$id]=$command
					unset with tmz pfz
					[[ $options =~ timeout=([0-9]+) ]] && tmz=${BASH_REMATCH[1]} || tmz=$default_timeout
					[[ $options =~ worker=([^ ]+) ]] && pfz=${BASH_REMATCH[1]} || pfz=$default_workers
					if (( $tmz )); then
						tmout[$id]=$tmz
						tmdue[$id]=$(($(date +%s)+$tmz))
						with+=${with:+ }timeout=$tmz
					fi
					if [[ $pfz ]]; then
						prefer[$id]=$pfz
						with+=${with:+ }prefer=$pfz
					fi
					queue+=($id)
					echo "$requester << accept request $id {$command}"
					log "accept request $id {$command} ${with:+with ${with// /,} }from $requester and" \
					    "enqueue $id, queue = ($(list_omit ${queue[@]}))"
				else
					echo "$requester << reject request $id {$command}"
					log "reject request $id {$command} from $requester since id $id has been occupied"
				fi
			else
				echo "$requester << reject request {$command}"
				log "reject request {$command} from $requester due to capacity," \
				    "#cmd = ${#cmd[@]}, queue = ($(list_omit ${queue[@]}))"
			fi

		elif [[ $message =~ $regex_response ]]; then
			worker=${BASH_REMATCH[1]}
			id=${BASH_REMATCH[2]}
			code=${BASH_REMATCH[3]}
			output=${BASH_REMATCH[4]}

			if [ "${assign[$id]}" == "$worker" ]; then
				unset assign[$id] tmdue[$id]
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
			stat=${BASH_REMATCH[2]}
			load=${BASH_REMATCH[3]}
			echo "$worker << confirm state $stat $load"
			log "confirm that $worker state $stat $load"
			current_stat=${state[$worker]:0:4}
			[ "$current_stat" == "hold" ] && stat="hold"
			state[$worker]=$stat:$load
			if [ "$current_stat" != "$stat" ] && (( ${#notify[$stat]} )); then
				for subscriber in ${notify[$stat]}; do
					echo "$subscriber << notify $worker state $stat"
				done
				log "state has been changed, notify ${notify[$stat]}"
			fi
			notify_capacity

		elif [[ $message =~ $regex_confirm ]]; then
			name=${BASH_REMATCH[1]}
			confirm=${BASH_REMATCH[2]}
			type=${BASH_REMATCH[3]}
			id=${BASH_REMATCH[4]}

			if [[ $id =~ ^[0-9]+$ ]]; then
				ids=($id)
				if [ "$type" == "response" ]; then
					who=${own[$id]}
				elif [ "$type" == "request" ] || [ "$type" == "terminate" ]; then
					who=${assign[$id]}
				fi
			else
				regex=${id}
				regex=${regex//\*/.*}
				regex=${regex//\?/.}
				regex=^$regex=$name$
				if [ "$type" == "response" ]; then
					ids=($(for id in ${!own[@]}; do
						[[ $id=${own[$id]} =~ $regex ]] && echo $id
					done | sort))
				elif [ "$type" == "request" ] || [ "$type" == "terminate" ]; then
					ids=($(for id in ${!assign[@]}; do
						[[ $id=${assign[$id]} =~ $regex ]] && echo $id
					done | sort))
				fi
				(( ${#ids[@]} )) && who=$name || who=
			fi

			if [ "$who" == "$name" ]; then
				for id in ${ids[@]}; do
					if [ "$type" == "request" ] && [ "${state[$name]:0:4}" == "hold" ]; then
						state[$name]="held":${state[$name]:5}
					elif [ "$type" == "terminate" ] && [[ -v own[$id] ]]; then
						echo "${own[$id]} << $confirm terminate $id"
					fi

					if [ "$confirm" == "accept" ] || [ "$confirm" == "confirm" ]; then
						log "confirm that $name ${confirm}ed $type $id"
						if [ "$type" == "request" ]; then
							if [[ -v news[assign-${own[$id]}] ]]; then
								echo "${own[$id]} << notify assign request $id to $name"
								log "assigned request $id to $name, notify ${own[$id]}"
							fi
						elif [ "$type" == "response" ] || [ "$type" == "terminate" ]; then
							unset cmd[$id] own[$id] res[$id] tmdue[$id] tmout[$id] prefer[$id] assign[$id]
						fi

					elif [ "$confirm" == "reject" ]; then
						if [ "$type" == "request" ]; then
							unset assign[$id]
						elif [ "$type" == "response" ]; then
							[[ -v tmout[$id] ]] && tmdue[$id]=$(($(date +%s)+${tmout[$id]}))
							echo "$requester << accept request $id {${cmd[$id]}}"
						fi
						if [ "$type" != "terminate" ]; then
							queue=($id ${queue[@]})
							log "confirm that $name ${confirm}ed $type $id and re-enqueue $id, queue = ($(list_omit ${queue[@]}))"
						fi
					fi
				done
			elif [ "$who" ]; then
				log "ignore that $name ${confirm}ed $type $id since it is owned by $who"
			else
				log "ignore that $name ${confirm}ed $type $id since no such $type"
			fi

		elif [[ $message =~ $regex_chat_system ]]; then
			type=${BASH_REMATCH[1]}
			message=${BASH_REMATCH[2]}

			if [ "$type" == "#" ]; then
				regex_logout="^logout: (\S+)$"
				regex_rename="^name: (\S+) becomes (\S+)$"

				if [[ $message =~ $regex_logout ]]; then
					name=${BASH_REMATCH[1]}
					log "$name logged out"
					if [[ -v state[$name] ]]; then
						unset state[$name]
						log "discard the worker state of $name"
						for id in ${!assign[@]}; do
							if [ "${assign[$id]}" == "$name" ]; then
								unset assign[$id]
								queue=($id ${queue[@]})
								log "revoke assigned request $id and re-enqueue $id, queue = ($(list_omit ${queue[@]}))"
							fi
						done
						notify_capacity
					fi
					for id in ${!own[@]}; do
						if [ "${own[$id]}" == "$name" ] && ! [ "${keep_unowned_tasks}" ]; then
							unset cmd[$id] own[$id] tmdue[$id] tmout[$id] prefer[$id]
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
								queue=($(erase_from queue $id))
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
						state[$new_name]=${state[$old_name]}
						unset state[$old_name]
						log "transfer the worker state to $new_name"
						for id in ${!assign[@]}; do
							if [ "${assign[$id]}" == "$old_name" ]; then
								log "transfer the ownership of assignment $id"
								assign[$id]=$new_name
							fi
						done
					fi
					for id in ${!own[@]}; do
						if [ "${own[$id]}" == "$old_name" ]; then
							log "transfer the ownerships of request $id and response $id"
							own[$id]=$new_name
						fi
					done
					for item in ${!notify[@]}; do
						if [[ " ${notify[$item]} " == *" $old_name "* ]]; then
							log "transfer the $item subscription to $new_name"
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
					return 1
				elif [[ "$message" == "name"* ]]; then
					log "registered as $broker successfully"
					if [ "$workers" ]; then
						for worker in ${workers//:/ }; do
							echo "$worker << query state"
						done
						log "query states from ${workers//:/ }"
						unset workers
					fi
				elif [[ "$message" == "failed name"* ]]; then
					log "another $broker is already running? exit"
					return 2
				fi
			fi

		elif [[ $message =~ $regex_others ]]; then
			name=${BASH_REMATCH[1]}
			command=${BASH_REMATCH[2]}
			options=${BASH_REMATCH[3]}

			if [ "$command" == "query" ]; then
				if [ "$options" == "protocol" ]; then
					echo "$name << protocol 0"
					log "accept query protocol from $name"

				elif [ "$options" == "capacity" ]; then
					echo "$name << capacity = $capacity"
					observe_worker_capacity raw
					echo "$name << worker_capacity = ${worker_capacity[@]}"
					log "accept query capacity from $name, capacity = $capacity," \
					    "worker_capacity = ($(list_omit ${worker_capacity[@]}))"

				elif [ "$options" == "queue" ]; then
					echo "$name << queue = (${queue[@]})"
					log "accept query queue from $name, queue = ($(list_omit ${queue[@]}))"

				elif [[ "$options" =~ ^(state)s?$ ]]; then
					status=()
					for worker in ${!state[@]}; do
						status+=("[$worker]=${state[$worker]}")
					done
					echo "$name << state = (${status[@]})"
					log "accept query state from $name, state = ($(list_omit ${status[@]}))"

				elif [[ "$options" =~ ^(assign(ment)?)s?$ ]]; then
					assignment=()
					for id in ${!assign[@]}; do
						[ "${own[$id]}" == "$name" ] && assignment+=("[$id]=${assign[$id]}")
					done
					echo "$name << assign = (${assignment[@]})"
					log "accept query assign from $name, assign = ($(list_omit ${assignment[@]}))"

				elif [[ "$options" =~ ^(worker)s?(.*)$ ]]; then
					workerx=(${BASH_REMATCH[2]:-$(<<< ${!state[@]} xargs -r printf "%s\n" | sort)})
					workerx=($(for worker in ${workerx[@]}; do [[ -v state[$worker] ]] && echo $worker; done))
					echo "$name << workers = (${workerx[@]})"
					for worker in ${workerx[@]}; do
						num_assign=0
						for id in ${!assign[@]}; do
							[ "${assign[$id]}" == "$worker" ] && num_assign=$((num_assign+1))
						done
						echo "$name << # $worker ${state[$worker]} $num_assign assigned"
					done
					log "accept query workers from $name, workers = ($(list_omit ${workerx[@]}))"

				elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [[ -v cmd[$id] ]] && echo $id; done))
					echo "$name << jobs = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v res[$id] ]]; then
							echo "$name << # $id {${cmd[$id]}} [${own[$id]}] = ${res[$id]%%:*} {${res[$id]#*:}}"
						elif [[ -v assign[$id] ]]; then
							echo "$name << # $id {${cmd[$id]}} [${own[$id]}] @ ${assign[$id]}"
						else
							rank=0
							while ! [ ${queue[$((rank++))]:-$id} == $id ]; do :; done
							echo "$name << # $id {${cmd[$id]}} [${own[$id]}] @ #$rank"
						fi
					done
					log "accept query jobs from $name, jobs = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$name" ] && ! [[ -v res[$id] ]] && echo $id; done))
					echo "$name << requests = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v assign[$id] ]]; then
							echo "$name << # request $id {${cmd[$id]}} @ ${assign[$id]}"
						else
							rank=0
							while ! [ ${queue[$((rank++))]:-$id} == $id ]; do :; done
							echo "$name << # request $id {${cmd[$id]}} @ #$rank"
						fi
					done
					log "accept query requests from $name, requests = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$name" ] && [[ -v res[$id] ]] && echo $id; done))
					echo "$name << responses = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$name << # response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
					log "accept query responses from $name, responses = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
					opts=($(list_args ${BASH_REMATCH[2]:-$(common_vars) ${set_var[@]} "$@"}))
					vars=()
					for opt in ${opts[@]}; do vars+=(${opt%%=*}); done
					echo "$name << options = (${vars[@]})"
					printf "$name << # %s\n" "${opts[@]}"
					log "accept query options from $name, options = ($(list_omit ${vars[@]}))"

				elif [ "$options" == "envinfo" ]; then
					echo "$name << accept query envinfo"
					log "accept query envinfo from $name"
					{
						envinfo=$(list_envinfo)
						echo "$name << result envinfo ($(<<<$envinfo wc -l))"
						<<< $envinfo xargs -r -d'\n' -L1 echo "$name << #"
					} &
				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "terminate" ]; then
				id=$options
				if [[ -v assign[$id] ]]; then
					if [ "$name" == "${own[$id]}" ]; then
						echo "${assign[$id]} << terminate $id"
						log "accept terminate $id from $name and forward it to ${assign[$id]}"
					else
						echo "$name << reject terminate $id"
						log "reject terminate $id from $name since it is owned by ${own[$id]}"
					fi
				elif [[ -v cmd[$id] ]]; then
					queue=($(erase_from queue $id))
					unset cmd[$id] own[$id] tmdue[$id] tmout[$id] prefer[$id]
					echo "$name << accept terminate $id"
					log "accept terminate $id from $name and remove it from queue"
				else
					echo "$name << reject terminate $id"
					log "reject terminate $id from $name since no such request"
				fi

			elif [ "$command" == "use" ]; then
				regex_use_protocol="^protocol (\S+)$"
				if [[ "$options" =~ $regex_use_protocol ]]; then
					protocol=${BASH_REMATCH[1]}
					if [ "$protocol" == "0" ]; then
						echo "$name << accept protocol $protocol"
						log "accept use protocol $protocol from $name"
					else
						echo "$name << reject protocol $protocol"
						log "reject use protocol $protocol from $name, unsupported protocol"
					fi
				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "subscribe" ]; then
				if [[ $options =~ ^(idle|busy|assign|capacity)$ ]]; then
					item=$options
					notify[$item]=$(printf "%s\n" ${notify[$item]} $name | sort | uniq)
					news[$item-$name]=subscribe
					echo "$name << accept $command $options"
					log "accept $command $options from $name"
					if [ "$item" == "idle" ] || [ "$item" == "busy" ]; then
						for worker in ${!state[@]}; do
							if [ "${state[$worker]%:*}" == "$item" ]; then
								echo "$name << notify $worker state $item"
							fi
						done
					elif [ "$item" == "capacity" ]; then
						observe_worker_capacity
						echo "$name << notify capacity ${worker_capacity[@]}"
					fi
				else
					echo "$name << reject $command $options"
					log "reject $command $options from $name, unsupported subscription"
				fi

			elif [ "$command" == "unsubscribe" ]; then
				if [[ $options =~ ^(idle|busy|assign|capacity)$ ]]; then
					item=$options
					notify[$item]=$(<<< ${notify[$item]} xargs -r printf "%s\n" | sed "/^${name}$/d")
					unset news[$item-$name]
					echo "$name << accept $command $options"
					log "accept $command $options from $name"
				else
					echo "$name << reject $command $options"
					log "reject $command $options from $name, unsupported subscription"
				fi

			elif [ "$command" == "set" ]; then
				var=(${options/=/ }); var=${var[0]}
				val=${options:$((${#var}+1))}
				set_var+=($var)
				echo "$name << accept set ${var}${val:+ ${val}}"
				declare val_old="${!var}" $var="$val"
				log "accept set ${var}${val:+=\"${val}\"} from $name"
				if [ "$var" == "broker" ]; then
					log "broker name has been changed, register $broker on the chat system..."
					echo "name $broker"
				elif [ "$var" == "capacity" ]; then
					notify_capacity
				fi

			elif [ "$command" == "unset" ]; then
				var=$options
				set_var+=($var)
				if [ "$var" ] && [ "$var" != "broker" ]; then
					echo "$name << accept unset $var"
					unset $var
					log "accept unset $var from $name"

				elif [ "$var" ]; then
					echo "$name << reject unset $var"
				fi

			elif [ "$command" == "operate" ]; then
				regex_operate_power="^(shutdown|restart) ?(.*)$"

				if [[ "$options" =~ $regex_operate_power ]]; then
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
							return 0
						elif [ "$type" == "restart" ]; then
							unset workers
							for worker in ${!state[@]}; do
								if ! [[ -v targets[$worker] ]]; then
									workers+=${workers:+:}$worker
								fi
							done
							log "$broker is restarting..."
							exec $0 $(list_args $(common_vars) ${set_var[@]} "$@" workers)
						fi
					fi
					unset targets
				else
					log "ignore $command $options from $name"
				fi

			elif [ "$command" == "shell" ]; then
				[[ $options =~ ^(\{(.+)\}|(.+))$ ]] && options=${BASH_REMATCH[2]:-${BASH_REMATCH[3]}}
				echo "$name << accept execute shell {$options}"
				log "accept execute shell {$options} from $name"
				{
					output=$(eval "$options" 2>&1)
					code=$?
					lines=$((${#output} ? $(<<<$output wc -l) : 0))
					echo "$name << result shell {$options} return $code ($lines)"
					echo -n "$output" | xargs -r -d'\n' -L1 echo "$name << #"
				} &

			else
				log "ignore $command $options from $name"
			fi

		elif ! [ "$message" ]; then
			current=$(date +%s)
			for id in ${!tmdue[@]}; do
				if (( $current > ${tmdue[$id]} )); then
					log "request $id failed due to timeout" \
					    "(due $(date '+%Y-%m-%d %H:%M:%S' -d @${tmdue[$id]})), notify ${own[$id]}"
					if [[ -v assign[$id] ]]; then
						echo "${assign[$id]} << terminate $id"
						log "terminate assigned request $id on ${assign[$id]}"
					elif [[ -v own[$id] ]]; then
						queue=($(erase_from queue $id))
					fi
					unset assign[$id] tmdue[$id]
					code="timeout"
					output=
					res[$id]=$code:$output
					echo "${own[$id]} << response $id $code {$output}"
				fi
			done
		fi

		if (( ${#queue[@]} )) && [[ ${state[@]} == *"idle"* ]]; then
			assign_queued_requests
		fi
	done

	log "message input is terminated, chat system is down?"
	return 16
}

assign_queued_requests() {
	declare -A workers_for cost_for
	local id worker stat pref workers_pref max_cost

	workers_for["*"]=$(for worker in ${!state[@]}; do
		stat=${state[$worker]%/*}
		[ ${stat:0:4} != "busy" ] && echo $worker:$stat
	done | sort -t':' -k3n -k2r | cut -d':' -f1)
	cost_for["*"]=$(extract_anchor_cost ${workers_for["*"]})

	for id in ${queue[@]}; do
		pref=${prefer[$id]:-"*"}
		if ! [[ -v workers_for[$pref] ]]; then
			workers_for[$pref]=$(for worker in ${workers_for["*"]}; do
				[[ $worker == $pref ]] && echo $worker
			done)
			cost_for[$pref]=$(extract_anchor_cost ${workers_for[$pref]})
		fi
		workers_pref=(${workers_for[$pref]})
		max_cost=${cost_for[$pref]:--1}

		for worker in ${workers_pref}; do
			stat=${state[$worker]%/*}
			(( ${stat:5} > $max_cost )) && break
			[ ${stat:0:4} != "idle" ] && continue

			echo "$worker << request $id {${cmd[$id]}}"
			log "assign request $id to $worker"
			state[$worker]="hold":${state[$worker]:5}
			assign[$id]=$worker
			queue=($(erase_from queue $id))
			id=; break
		done

		[[ $id ]] && workers_for[$pref]=
	done
}

extract_anchor_cost() {
	local src=(${@:-'?'})
	src=(${src[${load_balance_relax:-0}]} ${src[-1]})
	local cost=${state[$src]%/*}; cost=${cost:5}
	echo ${cost:--1}
}

observe_worker_capacity() {
	local current_worker_capacity_=${worker_capacity[@]}
	local capacity_details=
	worker_capacity=(0)
	for worker in ${!state[@]}; do
		local cap=${state[$worker]#*/}
		worker_capacity=$((worker_capacity+cap))
		capacity_details+=("[$worker]=$cap")
	done
	[ "$1" != "raw" ] && worker_capacity=$((worker_capacity < capacity ? worker_capacity : capacity))
	worker_capacity+=(${capacity_details[@]})
	local worker_capacity_=${worker_capacity[@]}
	[ "$worker_capacity_" != "$current_worker_capacity_" ]
	return $?
}

notify_capacity() {
	if (( ${#notify[capacity]} )) && observe_worker_capacity; then
		local subscriber
		for subscriber in ${notify[capacity]}; do
			echo "$subscriber << notify capacity ${worker_capacity[@]}"
		done
		log "capacity has been changed, notify ${notify[capacity]}"
	fi
}

common_vars() {
	echo broker capacity session logfile
}

list_args() {
	declare -A args
	for var in "$@"; do
		var=${var%%=*}
		[[ -v args[$var] ]] || echo $var="${!var}"
		args[$var]=${!var}
	done 2>/dev/null
}

list_omit() {
	if (( "$#" <= ${max_printable_list_size:-10} )); then
		echo "$@"
	else
		local num_show=${num_print_when_omitted:-6}
		echo "${@:1:$((num_show/2))}" "...[$(($#-num_show))]..." "${@:$#-$((num_show/2-1))}"
	fi
}

erase_from() {
	local list=${1:?}[@]
	local value=${2:?}
	list=" ${!list} "
	echo ${list/ $value / }
}

input() {
	unset ${1:-message}
	IFS= read -r -t ${input_timeout:-1} ${1:-message}
	return $(( $? < 128 ? $? : 0 ))
}

list_envinfo() (
	exec 2>/dev/null
	# host name
	echo "Host: $(hostname)"
	# OS name and version
	osinfo=$(uname -o 2>/dev/null | sed "s|GNU/||")
	osinfo+=" $(uname -r | sed -E 's/[^0-9.]+.+$//g')"
	if [[ $OSTYPE =~ cygwin|msys ]]; then
		ver=($(cmd /c ver 2>/dev/null | tr "[\r\n]" " "))
		(( ${#ver[@]} )) && osinfo+=" (Windows ${ver[-1]})"
	fi
	echo "OS: $osinfo"
	# CPU model
	cpuinfo=$(grep -m1 name /proc/cpuinfo | sed -E 's/.+:|\(\S+\)|CPU|[0-9]+-Core.+|@.+//g' | xargs)
	nodes=$(lscpu | grep 'NUMA node(s)' | cut -d: -f2 | xargs)
	if (( ${nodes:-1} != 1 )); then
		for (( i=0; i<${nodes:-1}; i++ )); do
			echo "CPU $i: $cpuinfo ($(taskset -c $(lscpu | grep 'NUMA node'$i' CPU' | cut -d: -f2) nproc)x)"
		done
	else
		echo "CPU: $cpuinfo ($(nproc --all)x)"
	fi
	# CPU affinity
	if [ "$(nproc)" != "$(nproc --all)" ]; then
		echo "CPU $(taskset -pc $$ | cut -d' ' -f4-) ($(nproc)x)"
	fi
	# GPU model
	nvidia-smi -L 2>/dev/null | sed -E "s/ \(UUID:.+$//g" | while IFS= read GPU; do echo "$GPU"; done
	# memory info
	size=($(head -n1 /proc/meminfo))
	size=$((${size[1]}0/1024/1024))
	size=$((size/10)).$((size%10))
	echo "RAM: $(printf "%.1fG" $size)"
)

#### script main routine ####
if [[ $1 != NC=* ]]; then
	trap 'cleanup 2>/dev/null; log "${broker:-broker} is terminated";' EXIT
	startup_broker "$@"
elif [[ $1 == NC=* ]]; then
	shift
	trap 'cleanup 2>/dev/null;' EXIT
fi
run_broker_main "$@"
