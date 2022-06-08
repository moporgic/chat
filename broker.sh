#!/bin/bash

broker_main() {
	declare "$@" >/dev/null 2>&1
	declare set_vars=("$@" broker capacity logfile)

	declare broker=${broker:-broker}
	declare capacity=${capacity-65536}
	declare default_timeout=${default_timeout:-0}
	declare default_workers=${default_workers}
	declare logfile=${logfile}

	log "broker version 2022-06-08 (protocol 0)"
	list_args "${set_vars[@]}" | xargs_ log "option:"
	list_envinfo | xargs_ log "platform"

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

	declare id_next
	declare io_count
	declare tcp_fd

	while init_system_io; do
		broker_routine "$@"
		local code=$?
		(( $code < 16 )) && break
	done

	return $code
}

broker_routine() {
	declare -a overview=() # idle 16/128 48/65536 16+32+0
	declare -a system_state=() # idle 16/128
	declare -a system_status=() # idle 16/128 48/65536 16+32+0 [A]=idle:2/4 [B]=idle:8/16 ...
	declare -a system_capacity=() # 128 65536 [A]=4 [B]=16 ...
	declare -a size_details=() # [A]=4 [B]=16 ...
	declare -a load_details=() # [A]=2/4 [B]=8/16 ...
	declare -a stat_details=() # [A]=idle:2/4 [B]=idle:8/16 ...

	log "verify chat system protocol 0..."
	echo "protocol 0"

	local regex_request="^(\S+) >> request ((([0-9]+) )?\{(.+)\}( with( ([^{}]+)| ?))?|(.+))$"
	local regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
	local regex_confirm="^(\S+) >> (accept|reject|confirm) (request|response|terminate) (\S+)$"
	local regex_worker_state="^(\S+) >> state (idle|busy) (\S+/\S+)( \((.*)\))?$"
	local regex_others="^(\S+) >> (query|terminate|operate|shell|set|unset|use|subscribe|unsubscribe) (.+)$"
	local regex_chat_system="^(#|%) (.+)$"

	local message
	while input message; do
		if [[ $message =~ $regex_request ]]; then
			local requester=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[4]}
			local command=${BASH_REMATCH[5]:-${BASH_REMATCH[9]}}
			local options=${BASH_REMATCH[8]}

			if [ "${overview:-full}" != "full" ]; then
				if [[ $id ]]; then
					local reply="$id"
				else
					id=${id_next:-1}
					while [[ -v own[$id] ]]; do id=$((id+1)); done
					local reply="$id {$command}"
				fi
				if ! [[ -v own[$id] ]]; then
					own[$id]=$requester
					cmd[$id]=$command
					local with= tmz= pfz=
					[[ $options =~ timeout=([0-9]+.*) ]] && tmz=${BASH_REMATCH[1]} || tmz=$default_timeout
					[[ $options =~ worker=([^ ]+) ]] && pfz=${BASH_REMATCH[1]} || pfz=$default_workers
					if [[ ${tmz:-0} != 0* ]]; then
						with+=${with:+ }timeout=$tmz
						[[ $tmz =~ ^([0-9]+)([^0-9]*)$ ]]
						tmz=${BASH_REMATCH[1]}
						case ${BASH_REMATCH[2]:-s} in
							h*) tmz=$((tmz*3600))000; ;;
							ms) :; ;;
							m*) tmz=$((tmz*60))000; ;;
							*)  tmz=${tmz}000; ;;
						esac
						tmout[$id]=$tmz
						tmdue[$id]=$(($(date +%s%3N)+$tmz))
					fi
					if [[ $pfz ]]; then
						with+=${with:+ }worker=$pfz
						prefer[$id]=$pfz
					fi
					queue+=($id)
					id_next=$((id+1))
					echo "$requester << accept request $reply"
					log "accept request $id {$command} ${with:+with ${with// /,} }from $requester and" \
					    "enqueue $id, queue = ($(list_omit ${queue[@]}))"
				else
					echo "$requester << reject request $reply"
					log "reject request $id {$command} from $requester since id $id has been occupied"
				fi
			else
				echo "$requester << reject request ${id:-{$command\}}"
				log "reject request ${id:+$id }{$command} from $requester due to capacity," \
				    "#cmd = ${#cmd[@]}, queue = ($(list_omit ${queue[@]}))"
			fi

		elif [[ $message =~ $regex_response ]]; then
			local worker=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[2]}
			local code=${BASH_REMATCH[3]}
			local output=${BASH_REMATCH[4]}

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
				echo "$worker << accept response $id"
				if [ "${assign[$id]}" ]; then
					log "ignore response $id $code {$output} from $worker since it is owned by ${assign[$id]}"
				else
					log "ignore response $id $code {$output} from $worker since no such assignment"
				fi
			fi

		elif [[ $message =~ $regex_worker_state ]]; then
			local worker=${BASH_REMATCH[1]}
			local stat=${BASH_REMATCH[2]} stat_last=${state[$worker]:0:4}
			local load=${BASH_REMATCH[3]}
			echo "$worker << confirm state $stat $load"
			log "confirm that $worker state $stat $load"
			[ "$stat_last" == "hold" ] && stat="hold"
			state[$worker]=$stat:$load
			if [ "$stat_last" != "$stat" ] && (( ${#notify[$stat]} )); then
				printf "%s << notify $worker state $stat\n" ${notify[$stat]}
				log "state has been changed, notify ${notify[$stat]}"
			fi

		elif [[ $message =~ $regex_confirm ]]; then
			local who=${BASH_REMATCH[1]}
			local confirm=${BASH_REMATCH[2]}
			local type=${BASH_REMATCH[3]}
			local id=${BASH_REMATCH[4]}

			if [[ $id =~ ^[0-9]+$ ]]; then
				local ids=($id)
				if [ "$type" == "response" ]; then
					local owner=${own[$id]}
				elif [ "$type" == "request" ] || [ "$type" == "terminate" ]; then
					local owner=${assign[$id]}
				fi
			else
				local patt=$id=$who
				if [ "$type" == "response" ]; then
					local ids=($(for id in ${!own[@]}; do
						[[ $id=${own[$id]} == $patt ]] && echo $id
					done | sort))
				elif [ "$type" == "request" ] || [ "$type" == "terminate" ]; then
					local ids=($(for id in ${!assign[@]}; do
						[[ $id=${assign[$id]} =~ $patt ]] && echo $id
					done | sort))
				fi
				local owner=
				(( ${#ids[@]} )) && owner=$who
			fi

			if [ "$owner" == "$who" ]; then
				for id in ${ids[@]}; do
					if [ "$type" == "request" ] && [ "${state[$who]:0:4}" == "hold" ]; then
						state[$who]="held":${state[$who]:5}
					elif [ "$type" == "terminate" ] && [[ -v own[$id] ]]; then
						echo "${own[$id]} << $confirm terminate $id"
					fi

					if [ "$confirm" == "accept" ] || [ "$confirm" == "confirm" ]; then
						log "confirm that $who ${confirm}ed $type $id"
						if [ "$type" == "request" ]; then
							if [[ -v news[assign-${own[$id]}] ]]; then
								echo "${own[$id]} << notify assign request $id to $who"
								log "assigned request $id to $who, notify ${own[$id]}"
							fi
						elif [ "$type" == "response" ] || [ "$type" == "terminate" ]; then
							unset cmd[$id] own[$id] res[$id] tmdue[$id] tmout[$id] prefer[$id] assign[$id]
						fi

					elif [ "$confirm" == "reject" ]; then
						if [ "$type" == "request" ]; then
							unset assign[$id]
						elif [ "$type" == "response" ]; then
							[[ -v tmout[$id] ]] && tmdue[$id]=$(($(date +%s%3N)+${tmout[$id]}))
							unset res[$id]
							echo "$requester << accept request $id"
						fi
						if [ "$type" != "terminate" ]; then
							queue=($id ${queue[@]})
							log "confirm that $who ${confirm}ed $type $id and re-enqueue $id, queue = ($(list_omit ${queue[@]}))"
						fi
					fi
				done

			elif [ "$owner" ]; then
				log "ignore that $who ${confirm}ed $type $id since it is owned by $owner"
			else
				log "ignore that $who ${confirm}ed $type $id since no such $type"
			fi

		elif [[ $message =~ $regex_chat_system ]]; then
			local type=${BASH_REMATCH[1]}
			local info=${BASH_REMATCH[2]}

			if [ "$type" == "#" ]; then
				local regex_logout="^logout: (\S+)$"
				local regex_rename="^name: (\S+) becomes (\S+)$"

				if [[ $info =~ $regex_logout ]]; then
					local who=${BASH_REMATCH[1]}
					log "$who logged out"
					if [[ -v state[$who] ]]; then
						discard_workers $who
					fi
					local id
					for id in ${!own[@]}; do
						if [ "${own[$id]}" == "$who" ] && ! [ "${keep_unowned_tasks}" ]; then
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
					local item
					for item in ${!notify[@]}; do
						if [[ " ${notify[$item]} " == *" $who "* ]]; then
							notify[$item]=$(printf "%s\n" ${notify[$item]} | sed "/^${who}$/d")
							unset news[$item-$who]
							log "unsubscribe $item for $who"
						fi
					done

				elif [[ $info =~ $regex_rename ]]; then
					local who=${BASH_REMATCH[1]}
					local new=${BASH_REMATCH[2]}
					if [ "$new" != "$broker" ]; then
						log "$who renamed as $new"
						if [[ -v state[$who] ]]; then
							state[$new]=${state[$who]}
							unset state[$who]
							log "transfer the worker state to $new"
							local id
							for id in ${!assign[@]}; do
								if [ "${assign[$id]}" == "$who" ]; then
									log "transfer the ownership of assignment $id"
									assign[$id]=$new
								fi
							done
						fi
						local id
						for id in ${!own[@]}; do
							if [ "${own[$id]}" == "$who" ]; then
								log "transfer the ownerships of request $id and response $id"
								own[$id]=$new
							fi
						done
						local item
						for item in ${!notify[@]}; do
							if [[ " ${notify[$item]} " == *" $who "* ]]; then
								log "transfer the $item subscription to $new"
								notify[$item]=$(printf "%s\n" ${notify[$item]} $new | sed "/^${who}$/d")
								news[$item-$new]=${news[$item-$who]}
								unset news[$item-$who]
							fi
						done
					fi
				fi

			elif [ "$type" == "%" ]; then
				if [[ "$info" == "protocol"* ]]; then
					log "chat system protocol verified successfully"
					log "register $broker on the chat system..."
					echo "name $broker"
				elif [[ "$info" == "failed protocol"* ]]; then
					log "unsupported protocol; shutdown"
					return 1
				elif [[ "$info" == "name"* ]]; then
					log "registered as $broker successfully"
					if [ "$workers" ]; then
						contact_workers ${workers[@]//:/ }
					fi
				elif [[ "$info" == "failed name"* ]]; then
					log "another $broker is already running? shutdown"
					return 2
				fi
			fi

		elif [[ $message =~ $regex_others ]]; then
			local who=${BASH_REMATCH[1]}
			local command=${BASH_REMATCH[2]}
			local options=${BASH_REMATCH[3]}

			if [ "$command" == "query" ]; then
				if [ "$options" == "protocol" ]; then
					echo "$who << protocol 0"
					log "accept query protocol from $who"

				elif [ "$options" == "overview" ]; then
					observe_overview
					echo "$who << overview = ${overview[@]}"
					log "accept query overview from $who, overview = ${overview[@]}"

				elif [ "$options" == "capacity" ]; then
					observe_overview; observe_capacity
					echo "$who << capacity = ${system_capacity[@]:0:2} (${system_capacity[@]:2})"
					log "accept query capacity from $who, capacity = ${system_capacity[@]:0:2}" \
					    "($(list_omit ${system_capacity[@]:2}))"

				elif [ "$options" == "queue" ]; then
					echo "$who << queue = (${queue[@]})"
					log "accept query queue from $who, queue = ($(list_omit ${queue[@]}))"

				elif [[ "$options" =~ ^(states?|status)$ ]]; then
					observe_overview; observe_status
					echo "$who << state = ${system_status[@]:0:4} (${system_status[@]:4})"
					log "accept query state from $who," \
					    "state = ${system_status[@]:0:4} ($(list_omit ${system_status[@]:4}))"

				elif [[ "$options" =~ ^(assign(ment)?)s?$ ]]; then
					local assignment=() id
					for id in ${!assign[@]}; do
						[ "${own[$id]}" == "$who" ] && assignment+=("[$id]=${assign[$id]}")
					done
					echo "$who << assign = (${assignment[@]})"
					log "accept query assign from $who, assign = ($(list_omit ${assignment[@]}))"

				elif [[ "$options" =~ ^(worker)s?(.*)$ ]]; then
					local workers=() worker
					workers=(${BASH_REMATCH[2]:-$(<<< ${!state[@]} xargs -r printf "%s\n" | sort)})
					workers=($(for worker in ${workers[@]}; do [[ -v state[$worker] ]] && echo $worker; done))
					echo "$who << workers = (${workers[@]})"
					for worker in ${workers[@]}; do
						local num_assign=$(<<<" ${assign[@]} " grep -o " $worker " | wc -l)
						echo "$who << # $worker ${state[$worker]} $num_assign assigned"
					done
					log "accept query workers from $who, workers = ($(list_omit ${workers[@]}))"

				elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [[ -v cmd[$id] ]] && echo $id; done))
					echo "$who << jobs = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v res[$id] ]]; then
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}] = ${res[$id]%%:*} {${res[$id]#*:}}"
						elif [[ -v assign[$id] ]]; then
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}] @ ${assign[$id]}"
						else
							local rank=0
							while ! [ ${queue[$((rank++))]:-$id} == $id ]; do :; done
							echo "$who << # $id {${cmd[$id]}} [${own[$id]}] @ #$rank"
						fi
					done
					log "accept query jobs from $who, jobs = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$who" ] && ! [[ -v res[$id] ]] && echo $id; done))
					echo "$who << requests = (${ids[@]})"
					for id in ${ids[@]}; do
						if [[ -v assign[$id] ]]; then
							echo "$who << # request $id {${cmd[$id]}} @ ${assign[$id]}"
						else
							local rank=0
							while ! [ ${queue[$((rank++))]:-$id} == $id ]; do :; done
							echo "$who << # request $id {${cmd[$id]}} @ #$rank"
						fi
					done
					log "accept query requests from $who, requests = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -r printf "%d\n" | sort -n)})
					ids=($(for id in ${ids[@]}; do [ "${own[$id]}" == "$who" ] && [[ -v res[$id] ]] && echo $id; done))
					echo "$who << responses = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$who << # response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
					log "accept query responses from $who, responses = ($(list_omit ${ids[@]}))"

				elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
					local vars=() args=()
					list_args ${BASH_REMATCH[2]:-${set_vars[@]}} >/dev/null
					echo "$who << options = (${vars[@]})"
					[[ ${args[@]} ]] && printf "$who << # %s\n" "${args[@]}"
					log "accept query options from $who, options = ($(list_omit ${vars[@]}))"

				elif [ "$options" == "envinfo" ]; then
					echo "$who << accept query envinfo"
					log "accept query envinfo from $who"
					{
						local envinfo=$(list_envinfo)
						echo "$who << result envinfo ($(<<<$envinfo wc -l))"
						<<< $envinfo xargs -r -d'\n' -L1 echo "$who << #"
					} &
				else
					log "ignore $command $options from $who"
				fi

			elif [ "$command" == "terminate" ]; then
				local id=$options
				if [[ -v assign[$id] ]]; then
					if [ "$who" == "${own[$id]}" ]; then
						echo "${assign[$id]} << terminate $id"
						log "accept terminate $id from $who and forward it to ${assign[$id]}"
					else
						echo "$who << reject terminate $id"
						log "reject terminate $id from $who since it is owned by ${own[$id]}"
					fi
				elif [[ -v cmd[$id] ]]; then
					queue=($(erase_from queue $id))
					unset cmd[$id] own[$id] tmdue[$id] tmout[$id] prefer[$id]
					echo "$who << accept terminate $id"
					log "accept terminate $id from $who and remove it from queue"
				else
					echo "$who << reject terminate $id"
					log "reject terminate $id from $who since no such request"
				fi

			elif [ "$command" == "use" ]; then
				local regex_use_protocol="^protocol (\S+)$"
				if [[ "$options" =~ $regex_use_protocol ]]; then
					local protocol=${BASH_REMATCH[1]}
					if [ "$protocol" == "0" ]; then
						echo "$who << accept protocol $protocol"
						log "accept use protocol $protocol from $who"
					else
						echo "$who << reject protocol $protocol"
						log "reject use protocol $protocol from $who, unsupported protocol"
					fi
				else
					log "ignore $command $options from $who"
				fi

			elif [ "$command" == "subscribe" ]; then
				if [[ $options =~ ^(state|status|idle|busy|assign|capacity)$ ]]; then
					local item=$options
					notify[$item]=$(printf "%s\n" ${notify[$item]} $who | sort | uniq)
					news[$item-$who]=subscribe
					echo "$who << accept $command $options"
					log "accept $command $options from $who"
					if [ "$item" == "idle" ] || [ "$item" == "busy" ]; then
						local worker
						for worker in ${!state[@]}; do
							if [ "${state[$worker]%:*}" == "$item" ]; then
								echo "$who << notify $worker state $item"
							fi
						done
					elif [ "$item" == "state" ]; then
						observe_overview; observe_state
						echo "$who << notify state ${system_state[@]}"
					elif [ "$item" == "status" ]; then
						observe_overview; observe_status
						echo "$who << notify state ${system_status[@]}"
					elif [ "$item" == "capacity" ]; then
						observe_overview; observe_capacity
						echo "$who << notify capacity ${system_capacity[@]}"
					fi
				else
					echo "$who << reject $command $options"
					log "reject $command $options from $who, unsupported subscription"
				fi

			elif [ "$command" == "unsubscribe" ]; then
				if [[ $options =~ ^(state|status|idle|busy|assign|capacity)$ ]]; then
					local item=$options
					notify[$item]=$(<<< ${notify[$item]} xargs -r printf "%s\n" | sed "/^${who}$/d")
					unset news[$item-$who]
					echo "$who << accept $command $options"
					log "accept $command $options from $who"
				else
					echo "$who << reject $command $options"
					log "reject $command $options from $who, unsupported subscription"
				fi

			elif [ "$command" == "set" ]; then
				local var=(${options/=/ })
				local val=${options:${#var}+1}
				local show_val="$var[@]"
				local val_old="${!show_val}"
				eval $var="$val"
				echo "$who << accept set ${var}${val:+=${val}}"
				log "accept set ${var}${val:+=\"${val}\"} from $who"
				set_vars+=($var)

				if [ "$var" == "broker" ]; then
					log "broker who has been changed, register $broker on the chat system..."
					echo "who $broker"
				elif [ "$var" == "workers" ]; then
					contact_workers ${workers[@]//:/ }
				fi

			elif [ "$command" == "unset" ]; then
				local regex_forbidden_unset="^(broker)$"
				local var=(${options/=/ })
				set_vars+=($var)

				if [ "$var" ] && ! [[ $var =~ $regex_forbidden_unset ]]; then
					echo "$who << accept unset $var"
					eval $var=
					log "accept unset $var from $who"

				elif [ "$var" ]; then
					echo "$who << reject unset $var"
				fi

			elif [ "$command" == "operate" ]; then
				local regex_operate_power="^(shutdown|restart) ?(.*)$"
				local regex_operate_workers="^(contact|discard) ?(.*)$"

				if [[ "$options" =~ $regex_operate_power ]]; then
					local type=${BASH_REMATCH[1]}
					local patt=${BASH_REMATCH[2]:-$broker}
					local matches=() match client
					matches=( $(<<<$patt grep -Eo '\S+' | while IFS= read -r match; do
						for client in ${!state[@]} $broker; do
							[[ $client == $match ]] && echo $client
						done
					done) )
					declare -A targets
					for match in ${matches[@]}; do targets[$match]=$type; done

					local target
					for target in ${!targets[@]}; do
						if [[ -v state[$target] ]]; then
							echo "$who << confirm $type $target"
							log "accept operate $type on $target from $who"
							echo "$target << operate $type"
						fi
					done
					if [[ -v targets[$broker] ]]; then
						echo "$who << confirm $type $broker"
						log "accept operate $type on $broker from $who"
						if [ "$type" == "shutdown" ]; then
							return 0
						elif [ "$type" == "restart" ]; then
							log "$broker is restarting..."
							local vars=() args=()
							list_args ${set_vars[@]} >/dev/null
							[[ $tcp_fd ]] && exec 0<&- 1>&-
							exec $0 "${args[@]}"
						fi
					fi
					unset targets

				elif [[ "$options" =~ $regex_operate_workers ]]; then
					local type=${BASH_REMATCH[1]}
					local patt=${BASH_REMATCH[2]:-"*"}

					if [ "$type" == "contact" ]; then
						log "accept operate $type $patt from $who"
						echo "$who << confirm $type $patt"
						contact_workers "$patt"

					elif [ "$type" == "discard" ]; then
						log "accept operate $type $patt from $who"
						local workers=() match client
						workers=( $(<<<$patt grep -Eo '\S+' | while IFS= read -r match; do
							for client in ${!state[@]}; do
								[[ $client == $match ]] && echo $client
							done
						done) )
						if [[ ${workers[@]} ]]; then
							printf "$who << confirm $type %s\n" ${workers[@]}
							discard_workers ${workers[@]}
						fi
					fi

				else
					log "ignore $command $options from $who"
				fi

			elif [ "$command" == "shell" ]; then
				[[ $options =~ ^(\{(.+)\}|(.+))$ ]] && options=${BASH_REMATCH[2]:-${BASH_REMATCH[3]}}
				echo "$who << accept execute shell {$options}"
				log "accept execute shell {$options} from $who"
				{
					local output code lines
					output=$(eval "$options" 2>&1)
					code=$?
					lines=$((${#output} ? $(<<<$output wc -l) : 0))
					echo "$who << result shell {$options} return $code ($lines)"
					echo -n "$output" | xargs -r -d'\n' -L1 echo "$who << #"
				} &

			else
				log "ignore $command $options from $who"
			fi

		elif ! [ "$message" ]; then
			current=$(date +%s%3N)
			local id
			for id in ${!tmdue[@]}; do
				if (( $current > ${tmdue[$id]} )); then
					local due=${tmdue[$id]}
					log "request $id failed due to timeout" \
					    "(due $(date '+%Y-%m-%d %H:%M:%S' -d @${due:0:-3}).${due: -3}), notify ${own[$id]}"
					if [[ -v assign[$id] ]]; then
						echo "${assign[$id]} << terminate $id"
						log "terminate assigned request $id on ${assign[$id]}"
					elif [[ -v own[$id] ]]; then
						queue=($(erase_from queue $id))
					fi
					unset assign[$id] tmdue[$id]
					local code="timeout"
					local output=
					res[$id]=$code:$output
					echo "${own[$id]} << response $id $code {$output}"
				fi
			done

		else
			log "ignore message: $message"
		fi

		if (( ${#queue[@]} )) && [[ ${state[@]} == *"idle"* ]]; then
			assign_queued_requests
		fi

		refresh_observations
	done

	log "message input is terminated, chat system is down?"
	return 16
}

assign_queued_requests() {
	declare -A workers_for cost_for
	local id worker stat pref workers max_cost

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
		workers=(${workers_for[$pref]})
		max_cost=${cost_for[$pref]:--1}

		for worker in ${workers[@]}; do
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

observe_overview() {
	local overview_last=${overview[@]}

	overview=() # idle 16/128 48/65536 16+32+0
	size_details=() # [A]=4 [B]=16 ...
	load_details=() # [A]=2/4 [B]=8/16 ...
	stat_details=() # [A]=idle:2/4 [B]=idle:8/16 ...

	local load_total=0 size_total=0
	local worker stat load size
	for worker in ${!state[@]}; do
		stat=${state[$worker]}
		load=${stat:5}
		load=(${load/\// })
		size=${load[1]}
		load_total=$((load_total+load))
		size_total=$((size_total+size))
		stat_details+=("[$worker]=$stat")
		load_details+=("[$worker]=$load/$size")
		size_details+=("[$worker]=$size")
	done

	local capacity=$capacity size_limit=$size_total
	[[ $capacity =~ ^[0-9]+$ ]] || capacity=$(($size_total $capacity))
	(( $size_limit >= $capacity )) && size_limit=$capacity

	local num_requests=$((${#cmd[@]} - ${#res[@]}))
	local num_assigned=$((num_requests - ${#queue[@]}))
	local stat="idle"
	(( $load_total >= $size_limit )) && stat="busy"
	(( $num_requests >= $capacity )) && stat="full"

	overview=(${stat} ${load_total}/${size_limit} \
	          ${num_requests}/${capacity} \
	          ${num_assigned}+${#queue[@]}+${#res[@]})

	local overview_this=${overview[@]}
	[ "$overview_this" != "$overview_last" ]
	return $?
}

observe_state() {
	local system_state_last=${system_state[@]}
	system_state=(${overview[@]:0:2})
	local system_state_this=${system_state[@]}
	[ "$system_state_this" != "$system_state_last" ]
	return $?
}

observe_status() {
	local system_status_last=${system_status[@]}
	system_status=(${overview[@]} "${stat_details[@]}")
	local system_status_this=${system_status[@]}
	[ "$system_status_this" != "$system_status_last" ]
	return $?
}

observe_capacity() {
	local system_capacity_last=${system_capacity[@]}
	local size_limit=${overview[1]#*/} capacity=${overview[2]#*/}
	system_capacity=(${size_limit} ${capacity} "${size_details[@]}")
	local system_capacity_this=${system_capacity[@]}
	[ "$system_capacity_this" != "$system_capacity_last" ]
	return $?
}

refresh_observations() {
	observe_overview
	if (( ${#notify[state]} )) && observe_state; then
		local notify_state=${system_state[@]}
		printf "%s << notify state $notify_state\n" ${notify[state]}
		log "state has been changed, notify ${notify[state]}"
	fi
	if (( ${#notify[status]} )) && observe_status; then
		local notify_status=${system_status[@]}
		printf "%s << notify state $notify_status\n" ${notify[status]}
		log "status has been changed, notify ${notify[status]}"
	fi
	if (( ${#notify[capacity]} )) && observe_capacity; then
		local notify_capacity=${system_capacity[@]}
		printf "%s << notify capacity $notify_capacity\n" ${notify[capacity]}
		log "capacity has been changed, notify ${notify[capacity]}"
	fi
}

contact_workers() {
	(( "$#" )) || return
	printf "%s << report state\n" "$@"
	log "contact $@ for worker state"
}

discard_workers() {
	local worker id
	for worker in "$@"; do
		unset state[$worker]
		log "discard the worker state of $worker"
		for id in ${!assign[@]}; do
			if [ "${assign[$id]}" == "$worker" ]; then
				unset assign[$id]
				queue=($id ${queue[@]})
				log "revoke assigned request $id and re-enqueue $id, queue = ($(list_omit ${queue[@]}))"
			fi
		done
		shift
	done
}

init_system_io() {
	io_count=${io_count:-0}
	if [[ $1 =~ ^([^:=]+):([0-9]+)$ ]]; then
		local addr=${BASH_REMATCH[1]}
		local port=${BASH_REMATCH[2]}
		local wait_for_conn=0
		while (( $((io_count++)) < ${max_io_count:-65536} )); do
			log "connect to chat system at $addr:$port..."
			sleep ${wait_for_conn:-0}
			if { exec {tcp_fd}<>/dev/tcp/$addr/$port; } 2>/dev/null; then
				log "connected to chat system successfully"
				exec 0<&$tcp_fd 1>&$tcp_fd {tcp_fd}>&- && return 0
			fi
			log "failed to connect $addr:$port, host down?"
			wait_for_conn=60
		done
	elif (( $((io_count++)) < ${max_io_count:-1} )); then
		return 0
	fi
	log "max number of connections is reached"
	return 16
}

list_args() {
	args=() vars=()
	local var val arg show=${show:0:3}
	show=${show:-arg}
	for var in "$@"; do
		var=${var%%=*}
		[[ " ${vars[@]} " == *" $var "* ]] && continue
		arg="${var}"
		if [[ $var =~ ^[a-zA-Z_][a-zA-Z_0-9]*$ ]]; then
			val="$var[@]"
			arg+="=${!val}"
		fi
		args+=("$arg")
		vars+=("$var")
		echo "${!show}"
	done
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

xargs_() {
	local line
	local perform="${@:-echo}"
	[[ $perform == *"{}"* ]] || perform+=" {}"
	perform=${perform//{\}/\$line}
	while IFS= read -r line; do eval "$perform"; done
}

input() {
	if read -r -t 0; then
		IFS= read -r ${1:-message}
		return $?
	else
		sleep ${system_tick:-0.1}
		eval ${1:-message}=
		return 0
	fi
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

log() {
	local name=$(basename "${0%.sh}")
	logfile=${logfile:-${name}_$(date '+%Y%m%d_%H%M%S_%3N').log}
	if exec 3>> "$logfile" && flock -xn 3; then
		trap 'code=$?; flock -u 3; exit $code' EXIT
		exec 2> >(trap '' INT TERM; exec tee /dev/fd/2 >&3)
	fi
	trap 'log "${'$name:-$name'} has been interrupted"; exit 64' INT
	trap 'log "${'$name:-$name'} has been terminated"; exit 64' TERM
	trap 'code=$?; cleanup 2>&-; log "${'$name:-$name'} is terminated"; exit $code' EXIT

	log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" >&2; }
	log "$@"
}

#### script main routine ####
if [ "$0" == "$BASH_SOURCE" ]; then
	broker_main "$@"
fi
