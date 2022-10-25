#!/bin/bash

main() {
	declare "$@" >/dev/null 2>&1
	declare configs=(name ${@%%=*} logfile)

	declare name=${name-node}
	declare brokers=(${brokers//:/ } ${broker//:/ })
	declare workers=(${workers//:/ } ${worker//:/ })
	declare capacity=${capacity-+65536}
	declare affinity=${affinity-$(nproc)}
	declare plugins=${plugins}
	declare logfile=${logfile}

	log "chat::node version 2022-10-20 (protocol 0)"
	args_of "${configs[@]}" | xargs_eval log "option:"
	envinfo | xargs_eval log "platform"
	xargs_eval -d: source {} >/dev/null <<< $plugins

	declare -A own # [id]=owner
	declare -A cmd # [id]=command
	declare -A res # [id]=code:output
	declare -A assign # [id]=worker
	declare -A prefer # [id]=worker
	declare -A tmout # [id]=timeout
	declare -A tmdue # [id]=due
	declare -A hdue # [id]=due
	declare -A pid # [id]=PID
	declare -A stdin # [id]=FD
	declare -A stdout # [id]=output
	declare -A state # [worker]=stat:load
	declare -A hold # [worker]=hold
	declare -A news # [type-who]=subscribe
	declare -A notify # [type]=subscriber...
	declare -a queue # id...

	declare id_next
	declare tcp_fd
	declare res_fd

	while init_system_io; do
		session "$@"
		local code=$?
		(( $code < 16 )) && break
	done
	return $code
}

session() {
	declare -a overview=() # idle 16/128 48/65536 16+32+0
	declare -a lastview=() # idle 16/128 48/65536 16+32+0
	declare -a system_state=() # idle 16/128
	declare -a system_status=() # idle 16/128 48/65536 16+32+0 [A]=idle:2/4 [B]=idle:8/16 ...
	declare -a system_capacity=() # 128 65536 [A]=4 [B]=16 ...
	declare -a size_details=() # [A]=4 [B]=16 ...
	declare -a load_details=() # [A]=2/4 [B]=8/16 ...
	declare -a stat_details=() # [A]=idle:2/4 [B]=idle:8/16 ...

	log "verify chat system protocol 0..."
	echo "protocol 0"

	local regex_request="^(\S+) >> request (([0-9]+) )?(\{(.*)\}( with( ([^{}]+)| ?))?|(.+))$"
	local regex_response="^(\S+) >> response (\S+) (\S+) \{(.*)\}$"
	local regex_terminate="^(\S+) >> terminate (\S+)$"
	local regex_confirm="^(\S+) >> (accept|reject|confirm) (state|request|response|terminate|protocol) (.+)$"
	local regex_worker_state="^(\S+) >> notify state (idle|busy|full) (\S+/\S+)( \((.*)\))?$"
	local regex_others="^(\S+) >> (query|report|operate|shell|set|unset|use|subscribe|unsubscribe) (.+)$"
	local regex_chat_system="^(#|%) (.+)$"
	local regex_request_input="^input (\{(.*)\}|(.+))$"

	local message
	while input message; do
		if [[ $message =~ $regex_worker_state ]]; then
			# ^(\S+) >> notify state (idle|busy|full) (\S+/\S+)( \((.*)\))?$
			local worker=${BASH_REMATCH[1]}
			local stat=${BASH_REMATCH[2]}
			local load=${BASH_REMATCH[3]}
			local owned=${BASH_REMATCH[4]}
			echo "$worker << confirm state $stat $load"

			if (( ! hold[$worker] )); then
				log "confirm that $worker state $stat $load"
				state[$worker]=$stat:$load
			else
				local load=(${load/\// })
				local size=${load[1]}
				load=$((load+hold[$worker]))
				(( load < size )) && stat="idle" || stat="busy"
				log "confirm that $worker state $stat $load/$size (${hold[$worker]} hold)"
				state[$worker]=$stat:$load/$size
			fi

			if [[ $owned ]]; then
				local ids=($(filter_keys assign $worker))
				erase_from ids ${owned:2:-1} ${!hdue[@]}
				if [[ ${ids[@]} ]]; then
					queue=(${ids[@]} ${queue[@]})
					log "confirm that $worker disowned request ${ids[@]}, queue = ($(omit ${queue[@]}))"
				fi
			fi

		elif [[ $message =~ $regex_confirm ]]; then
			# ^(\S+) >> (accept|reject|confirm) (state|request|response|terminate|protocol) (\S+)$
			local who=${BASH_REMATCH[1]}
			local confirm=${BASH_REMATCH[2]}
			local type=${BASH_REMATCH[3]}
			local option=${BASH_REMATCH[4]}
			local id=$option

			if [ "$type" == "state" ]; then
				log "confirm that $who ${confirm}ed state $option"

			elif [ "$type" == "request" ]; then
				local ids=()
				if [ "${assign[$id]}" == "$who" ] && [[ -v hdue[$id] ]]; then
					ids=($id)
				elif [[ $id == *[^0-9]* ]]; then
					ids=($(filter_keys assign "$id" "$who" | sort -n))
					retain_from ids ${!hdue[@]}
				fi
				if [[ ${ids[@]} ]] && [ "$confirm" == "accept" ]; then
					hold[$who]=$((hold[$who]-${#ids[@]}))
					log "confirm that $who ${confirm}ed $type ${ids[@]}"
					for id in ${ids[@]}; do
						unset hdue[$id]
						notify_assign_request $id $who
					done
				elif [[ ${ids[@]} ]] && [ "$confirm" == "reject" ]; then
					for id in ${ids[@]}; do
						unset hdue[$id] assign[$id]
					done
					adjust_worker_state $who -${#ids[@]}
					queue=(${ids[@]} ${queue[@]})
					log "confirm that $who ${confirm}ed $type ${ids[@]}, queue = ($(omit ${queue[@]}))"
				elif [[ -v stdin[$id] ]] && [ "$confirm" == "confirm" ]; then
					log "confirm that $who ${confirm}ed $type $id input"
				else
					log "ignore that $who ${confirm}ed $type $id since no such $type"
				fi

			elif [ "$type" == "response" ]; then
				local ids=()
				if [ "${own[$id]}" == "$who" ] && [[ -v res[$id] ]]; then
					ids=($id)
				elif [[ $id == *[^0-9]* ]]; then
					ids=($(filter_keys own "$id" "$who" | sort -n))
					retain_from ids ${!res[@]}
				fi
				if [[ ${ids[@]} ]] && [ "$confirm" == "accept" ]; then
					for id in ${ids[@]}; do
						unset res[$id] cmd[$id] own[$id] tmout[$id] prefer[$id] stdin[$id] stdout[$id]
					done
					log "confirm that $who ${confirm}ed $type ${ids[@]}"
				elif [[ ${ids[@]} ]] && [ "$confirm" == "reject" ]; then
					for id in ${ids[@]}; do
						unset res[$id]
						[[ -v stdin[$id] ]] && stdin[$id]=
						[[ -v stdout[$id] ]] && stdout[$id]=
						[[ -v tmout[$id] ]] && tmdue[$id]=$(($(date +%s%3N)+${tmout[$id]}))
						echo "$who << accept request $id"
					done
					queue+=(${ids[@]})
					log "confirm that $who ${confirm}ed $type ${ids[@]}, queue = ($(omit ${queue[@]}))"
				elif [[ -v stdout[$id] ]] && [ "$confirm" == "confirm" ]; then
					log "confirm that $who ${confirm}ed $type $id output"
				else
					log "ignore that $who ${confirm}ed $type $id since no such $type"
				fi

			elif [ "$type" == "terminate" ]; then
				local ids=() ida=()
				if [[ ! -v assign[$id] ]]; then
					ids=($id)
				elif [ "${assign[$id]}" == "$who" ]; then
					ids=($id)
					ida=($id)
				elif [[ $id == *[^0-9]* ]]; then
					ids=($(filter_keys assign "$id" "$who" | sort -n))
					retain_from ids ${!assign[@]}
					ida=(${ids[@]})
				fi
				if [[ ${ida[@]} ]] && ([ "$confirm" == "accept" ] || [ "$confirm" == "confirm" ]); then
					for id in ${ida[@]}; do
						unset assign[$id] hdue[$id]
					done
					queue=(${ida[@]} ${queue[@]})
					log "confirm that $who ${confirm}ed $type ${ids[@]}, queue = ($(omit ${queue[@]}))"
				elif [[ ${ids[@]} ]]; then
					log "confirm that $who ${confirm}ed $type ${ids[@]}"
				else
					log "ignore that $who ${confirm}ed $type $id since no such $type"
				fi

			elif [ "$type" == "protocol" ]; then
				if [ "$confirm" == "accept" ]; then
					log "handshake with $who successfully"
					subscribe state $who
					log "subscribed state for $who automatically"
					observe_state
					echo "$who << notify state ${system_state[@]}"

				elif [ "$confirm" == "reject" ]; then
					log "handshake failed, unsupported protocol; shutdown"
					return 2
				fi

			elif [ "$type" == "confirm" ]; then
				log "confirm that $who ${confirm}ed $type $option"
			else
				log "ignore that $who ${confirm}ed $type $option from $who"
			fi

		elif [[ $message =~ $regex_request ]]; then
			# ^(\S+) >> request (([0-9]+) )?(\{(.*)\}( with( ([^{}]+)| ?))?|(.+))$
			local owner=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[3]}
			local command=${BASH_REMATCH[5]:-${BASH_REMATCH[9]}}
			local options=${BASH_REMATCH[8]}

			local -A opts=()
			extract_options timeout workers enqueue input output

			if [[ ${overview:-full} != full ]] && [[ ! -v own[$id] ]] && [[ ! $options ]]; then
				local request="$id"
				if [[ ! $id ]]; then
					id=${id_next:-1}
					while [[ -v own[$id] ]]; do id=$((id+1)); done
					request="$id {$command}"
				fi
				own[$id]=$owner
				cmd[$id]=$command
				queue+=($id)
				if [[ ${opts[enqueue]} == preempt ]]; then
					local ids=() it=$id
					for id in ${queue[@]}; do
						[[ ${own[$id]} == $owner ]] && ids+=($it) it=$id || ids+=($id)
					done
					queue=(${ids[@]})
					options+="enqueue=${opts[enqueue]} "
				fi
				if [[ ${opts[timeout]} == [1-9]* ]]; then
					tmout[$id]=$(millisec ${opts[timeout]})
					tmdue[$id]=$(($(date +%s%3N)+tmout[$id]))
					options+="timeout=${opts[timeout]} "
				fi
				if [[ ${opts[workers]} ]]; then
					prefer[$id]=${opts[workers]}
					options+="workers=${opts[workers]} "
				fi
				if [[ -v opts[input] ]]; then
					stdin[$id]=
					options+="input "
				fi
				if [[ -v opts[output] ]]; then
					stdout[$id]=
					options+="output "
				fi
				if confirm_request $id; then
					id_next=$((id+1))
					echo "$owner << accept request $request"
					log "accept request $id {$command} ${options:+with $options}from $owner, queue = ($(omit ${queue[@]}))"
				else
					erase_from queue $id
					unset own[$id] cmd[$id] tmout[$id] tmdue[$id] prefer[$id] stdin[$id] stdout[$id]
					echo "$owner << reject request $request"
					log "reject request $id {$command} ${options:+with $options}from $owner due to policy"
				fi

			elif [[ -v stdin[$id] ]] && [[ $command =~ $regex_request_input ]]; then
				# ^input (\{(.*)\}|(.+))$
				local input=${BASH_REMATCH[2]:-${BASH_REMATCH[3]}}
				if [[ ${own[$id]} == $owner ]] && [[ -v assign[$id] ]] && [[ ! -v hdue[$id] ]]; then
					if [[ ${assign[$id]} != $name ]]; then
						echo "${assign[$id]} << request $id input {$input}"
						echo "$owner << confirm request $id"
						log "accept request $id input {$input} from $owner, forward to ${assign[$id]}"
					else
						echo "$input" >&${stdin[$id]}
						echo "$owner << confirm request $id"
						log "accept request $id input {$input} from $owner, input into $id"
					fi
				elif [[ ${own[$id]} != $owner ]]; then
					echo "$owner << reject request $id"
					log "reject request $id input {$input} from $owner since it is owned by ${own[$id]}"
				else
					echo "$owner << reject request $id"
					log "reject request $id input {$input} from $owner since id $id has not been assigned"
				fi

			elif [[ -v own[$id] ]]; then
				echo "$owner << reject request $id"
				log "reject request $id {$command} from $owner since id $id has been occupied"
			elif [[ $options ]]; then
				echo "$owner << reject request ${id:-{$command\}}"
				log "reject request ${id:+$id }{$command} from $owner due to unsupported option $options"
			else
				echo "$owner << reject request ${id:-{$command\}}"
				log "reject request ${id:+$id }{$command} from $owner due to capacity," \
				    "#cmd = ${#cmd[@]}, queue = ($(omit ${queue[@]}))"
			fi

		elif [[ $message =~ $regex_response ]]; then
			# ^(\S+) >> response (\S+) (\S+) \{(.*)\}$
			local worker=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[2]}
			local code=${BASH_REMATCH[3]}
			local output=${BASH_REMATCH[4]}

			if [[ -v cmd[$id] ]] && [ "${assign[$id]}" == "$worker" ]; then
				if [ $code != output ]; then
					res[$id]=$code:${stdout[$id]}$output
					[[ -v stdout[$id] ]] && stdout[$id]=
					unset assign[$id] tmdue[$id] hdue[$id]
					echo "$worker << accept response $id"
				else
					stdout[$id]+=$output\\n
					echo "$worker << confirm response $id"
				fi
				echo "${own[$id]} << response $id $code {$output}"
				log "accept response $id $code {$output} from $worker and forward it to ${own[$id]}"

			elif [[ ! -v cmd[$id] ]]; then
				echo "$worker << accept response $id"
				log "ignore response $id $code {$output} from $worker since no such request"
			elif [[ -v assign[$id] ]]; then
				echo "$worker << accept response $id"
				log "ignore response $id $code {$output} from $worker since it is owned by ${assign[$id]}"
			else
				echo "$worker << accept response $id"
				log "ignore response $id $code {$output} from $worker since no such assignment"
			fi

		elif [[ $message =~ $regex_terminate ]]; then
			# ^(\S+) >> terminate (\S+)$
			local who=${BASH_REMATCH[1]}
			local id=${BASH_REMATCH[2]}

			local ids=()
			if [ "${own[$id]}" == "$who" ] && [[ ! -v res[$id] ]]; then
				ids=($id)
			elif [[ $id == *[^0-9]* ]]; then
				ids=($(filter_keys own "$id" "$who" | sort -n))
				erase_from ids ${!res[@]}
			fi
			if [[ ${ids[@]} ]]; then
				printf "$who << accept terminate %d\n" ${ids[@]}
				terminate ${ids[@]}
				for id in ${ids[@]}; do
					unset cmd[$id] own[$id] tmout[$id] prefer[$id] stdin[$id] stdout[$id]
				done
				log "accept terminate ${ids[@]} from $who, queue = ($(omit ${queue[@]}))"

			elif [ "${own[$id]}" ] && [[ ! -v res[$id] ]]; then
				echo "$who << reject terminate $id"
				log "reject terminate $id from $who since it is owned by ${own[$id]}"
			else
				echo "$who << reject terminate $id"
				log "reject terminate $id from $who since no such request"
			fi

		elif [[ $message =~ $regex_others ]]; then
			# ^(\S+) >> (query|operate|shell|set|unset|use|subscribe|unsubscribe) (.+)$
			local who=${BASH_REMATCH[1]}
			local command=${BASH_REMATCH[2]}
			local options=${BASH_REMATCH[3]}

			if [ "$command" == "query" ]; then
				if [ "$options" == "protocol" ]; then
					echo "$who << protocol 0 version 2022-10-20"
					log "accept query protocol from $who"

				elif [ "$options" == "overview" ]; then
					echo "$who << overview = ${overview[@]}"
					log "accept query overview from $who, overview = ${overview[@]}"

				elif [ "$options" == "capacity" ]; then
					observe_capacity
					echo "$who << capacity = ${system_capacity[@]:0:2} (${system_capacity[@]:2})"
					log "accept query capacity from $who, capacity = ${system_capacity[@]:0:2}" \
					    "($(omit ${system_capacity[@]:2}))"

				elif [ "$options" == "queue" ]; then
					echo "$who << queue = (${queue[@]})"
					log "accept query queue from $who, queue = ($(omit ${queue[@]}))"

				elif [[ "$options" =~ ^(states?|status)$ ]]; then
					observe_status
					echo "$who << state = ${system_status[@]:0:4} (${system_status[@]:4})"
					log "accept query state from $who," \
					    "state = ${system_status[@]:0:4} ($(omit ${system_status[@]:4}))"

				elif [[ "$options" =~ ^(assign(ment)?)s?$ ]]; then
					local assignment=() id
					for id in ${!assign[@]}; do
						[ "${own[$id]}" == "$who" ] && assignment+=("[$id]=${assign[$id]}")
					done
					echo "$who << assign = (${assignment[@]})"
					log "accept query assign from $who, assign = ($(omit ${assignment[@]}))"

				elif [[ "$options" =~ ^(broker)s?(.*)$ ]]; then
					local query=() broker
					query=(${BASH_REMATCH[2]:-$(<<< ${notify[state]} xargs -rn1 | sort)})
					retain_from query ${notify[state]}
					echo "$who << brokers = (${query[@]})"
					for broker in ${query[@]}; do
						local owned=($(filter_keys own $broker))
						echo "$who << # $broker ${#owned[@]} owned"
					done
					log "accept query brokers from $who, brokers = ($(omit ${query[@]}))"

				elif [[ "$options" =~ ^(worker)s?(.*)$ ]]; then
					local query=() worker
					query=(${BASH_REMATCH[2]:-$(<<< ${!state[@]} xargs -rn1 | sort)})
					retain_from workers ${!state[@]}
					echo "$who << workers = (${query[@]})"
					for worker in ${workers[@]}; do
						local assigned=($(filter_keys assign $worker))
						echo "$who << # $worker ${state[$worker]} ${#assigned[@]} assigned"
					done
					log "accept query workers from $who, workers = ($(omit ${query[@]}))"

				elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -rn1 | sort -n)})
					retain_from ids ${!cmd[@]}
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
					log "accept query jobs from $who, jobs = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(request)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!cmd[@]} xargs -rn1 | sort -n)})
					retain_from ids $(filter_keys own $who)
					erase_from ids ${!res[@]}
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
					log "accept query requests from $who, requests = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -rn1 | sort -n)})
					retain_from ids $(filter_keys own $who)
					retain_from ids ${!res[@]}
					echo "$who << responses = (${ids[@]})"
					for id in ${ids[@]}; do
						echo "$who << # response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
					log "accept query responses from $who, responses = ($(omit ${ids[@]}))"

				elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
					local vars=() args=()
					args_of ${BASH_REMATCH[2]:-${configs[@]}} >/dev/null
					echo "$who << options = (${vars[@]})"
					[[ ${args[@]} ]] && printf "$who << # %s\n" "${args[@]}"
					log "accept query options from $who, options = ($(omit ${vars[@]}))"

				elif [ "$options" == "envinfo" ]; then
					local envinfo=$(envinfo)
					local envitem=$(cut -d: -f1 <<< "$envinfo" | xargs)
					echo "$who << envinfo = ($envitem)"
					<<< $envinfo xargs -r -d'\n' -L1 echo "$who << #"
					log "accept query envinfo from $who, envinfo = ($envitem)"

				else
					log "ignore $command $options from $who"
				fi &

			elif [ "$command" == "report" ]; then
				if [ "$options" == "state" ]; then
					log "accept report state from $who"
					observe_state
					echo "$who << notify state ${system_state[@]}"
				elif [ "$options" == "state requests" ]; then
					log "accept report state with requests from $who"
					observe_state
					echo "$who << notify state ${system_state[@]} (${!cmd[@]})"
				elif [ "$options" == "status" ]; then
					log "accept report status from $who"
					observe_status
					echo "$who << notify state ${system_status[@]}"
				elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
					local ids=() id
					ids=(${BASH_REMATCH[2]:-$(<<< ${!res[@]} xargs -rn1 | sort -n)})
					retain_from ids $(filter_keys own $who)
					retain_from ids ${!res[@]}
					log "accept report responses from $who, responses = ($(omit ${ids[@]}))"
					for id in ${ids[@]}; do
						echo "$who << response $id ${res[$id]%%:*} {${res[$id]#*:}}"
					done
				else
					log "ignore $command $options from $who"
				fi &

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
				if [[ $options =~ ^(state|status|idle|assign|capacity)$ ]]; then
					local item=$options
					subscribe $item $who
					echo "$who << accept $command $options"
					log "accept $command $options from $who"
					if [ "$item" == "idle" ]; then
						[ "${overview[0]}" == "$item" ] && echo "$who << notify state $item"
					elif [ "$item" == "state" ]; then
						observe_state
						echo "$who << notify state ${system_state[@]}"
					elif [ "$item" == "status" ]; then
						observe_status
						echo "$who << notify state ${system_status[@]}"
					elif [ "$item" == "capacity" ]; then
						observe_capacity
						echo "$who << notify capacity ${system_capacity[@]}"
					fi
				else
					echo "$who << reject $command $options"
					log "reject $command $options from $who, unsupported subscription"
				fi

			elif [ "$command" == "unsubscribe" ]; then
				if [[ $options =~ ^(state|status|idle|assign|capacity)$ ]]; then
					local item=$options
					unsubscribe $item $who
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
				eval $var="\"$val\""
				configs+=($var)
				echo "$who << accept set ${var}${val:+=${val}}"
				log "accept set ${var}${val:+=\"${val}\"} from $who"

				if [ "$var" == "name" ]; then
					log "node name has been changed, register $name on the chat system..."
					echo "name $name"
				elif [ "$var" == "brokers" ] || [ "$var" == "broker" ]; then
					brokers=${!var}; brokers=(${brokers//:/ })
					change_brokers "$val_old" "${brokers[@]}"
					erase_from configs brokers broker; configs+=(brokers)
				elif [ "$var" == "workers" ] || [ "$var" == "worker" ]; then
					workers=${!var}; workers=(${workers//:/ })
					change_workers "$val_old" "${workers[@]}"
					erase_from configs workers worker; configs+=(workers)
				elif [ "$var" == "capacity" ]; then
					set_capacity ${capacity}
				elif [ "$var" == "affinity" ]; then
					set_affinity ${affinity:-$(nproc)}
				elif [ "$var" == "plugins" ]; then
					xargs_eval -d: source {} >/dev/null <<< $plugins
				fi

			elif [ "$command" == "unset" ]; then
				local var=(${options/=/ })
				local show_val="$var[@]"
				local val_old="${!show_val}"
				eval $var=
				erase_from configs $var
				echo "$who << accept unset $var"
				log "accept unset $var from $who"

				if [ "$var" == "name" ]; then
					name=$(basename "${0%.sh}")
					log "node name has been changed, register $name on the chat system..."
					echo "name $name"
				elif [ "$var" == "brokers" ] || [ "$var" == "broker" ]; then
					brokers=()
					discard_brokers $val_old
					erase_from configs brokers broker
				elif [ "$var" == "workers" ] || [ "$var" == "worker" ]; then
					workers=()
					discard_workers $val_old
					erase_from configs workers worker
				elif [ "$var" == "capacity" ]; then
					set_capacity 0
				elif [ "$var" == "affinity" ]; then
					set_affinity 0
				fi

			elif [ "$command" == "operate" ]; then
				local regex_operate_power="^(shutdown|restart) ?(.*)$"
				local regex_operate_workers="^(contact|discard) ?(.*)$"

				if [[ "$options" =~ $regex_operate_power ]]; then
					local type=${BASH_REMATCH[1]}
					local patt=${BASH_REMATCH[2]:-$name}
					local matches=($(<<<$patt xargs -rn1 | xargs_eval "filter \"{}\" ${!state[@]} $name"))

					local match
					for match in ${matches[@]}; do
						if [[ -v state[$match] ]] && [ "$match" != "$name" ]; then
							echo "$who << confirm $type $match"
							log "accept operate $type on $match from $who"
							echo "$match << operate $type"
						fi
					done
					if [[ " ${matches[@]} " == *" $name "* ]]; then
						echo "$who << confirm $type $name"
						log "accept operate $type on $name from $who"
						if [ "$type" == "shutdown" ]; then
							declare -g name=$name
							kill_request ${pid[@]}
							return 0
						elif [ "$type" == "restart" ]; then
							log "${name:-node} is restarting..."
							kill_request ${pid[@]}
							local vars=() args=()
							args_of ${configs[@]} >/dev/null
							[[ $tcp_fd ]] && exec 0<&- 1>&-
							exec $0 "${args[@]}"
						fi
					fi

				elif [[ "$options" =~ $regex_operate_workers ]]; then
					local type=${BASH_REMATCH[1]}
					local patt=${BASH_REMATCH[2]:-"*"}

					if [ "$type" == "contact" ]; then
						log "accept operate $type $patt from $who"
						echo "$who << confirm $type $patt"
						contact_workers "$patt"

					elif [ "$type" == "discard" ]; then
						log "accept operate $type $patt from $who"
						local worker
						for worker in $(<<<$patt xargs -rn1 | xargs_eval "filter \"{}\" ${!state[@]}"); do
							printf "$who << confirm $type %s\n" $worker
							discard_workers $worker
						done
					fi

				# elif [[ "$options" == "broadcast "* ]]; then

				elif [[ "$options" == "plugin "* ]] || [[ "$options" == "source "* ]]; then
					local mode=${options:0:6}
					local plug=${options:7}
					log "accept operate $mode $plug from $who"
					echo "$who << confirm $mode $plug"
					source $plug >/dev/null 2>&1
					if [[ $mode == "plugin" ]] && [[ :$plugins: != *:$plug:* ]]; then
						plugins+=${plugins:+:}$plug
						log "confirm set plugins=\"$plugins\""
						configs+=("plugins")
					fi

				elif [[ "$options" == "output "* ]]; then
					local output=${options:7}
					echo "$output"
					log "accept operate output \"$output\" from $who"
					echo "$who << confirm output $output"

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

		elif [[ $message =~ $regex_chat_system ]]; then
			# ^(#|%) (.+)$
			local type=${BASH_REMATCH[1]}
			local info=${BASH_REMATCH[2]}

			if [ "$type" == "#" ]; then
				local regex_login="^login: (\S+)$"
				local regex_logout="^logout: (\S+)$"
				local regex_rename="^name: (\S+) becomes (\S+)$"

				if [[ $info =~ $regex_login ]]; then
					local who=${BASH_REMATCH[1]}

					if contains brokers $who; then
						log "broker $who connected"
						contact_brokers $new
					fi
					if contains workers $who; then
						log "worker $who connected"
						contact_workers $new
					fi

				elif [[ $info =~ $regex_logout ]]; then
					local who=${BASH_REMATCH[1]}

					log "$who logged out"
					if contains brokers $who; then
						log "broker $who disconnected, wait until $who come back..."
					elif contains workers $who; then
						log "worker $who disconnected, wait until $who come back..."
					fi
					if [[ -v state[$who] ]]; then
						discard_workers $who
					fi
					if contains own $who && [[ ! ${keep_unowned_tasks} ]]; then
						discard_owned_assets $(filter_keys own $who)
					fi
					local item
					for item in $(vfmt=" %s " filter_keys notify "* $who *"); do
						log "unsubscribe $item for $who"
						unsubscribe $item $who
					done

				elif [[ $info =~ $regex_rename ]]; then
					local who=${BASH_REMATCH[1]}
					local new=${BASH_REMATCH[2]}

					log "$who renamed as $new"

					if [[ -v state[$who] ]]; then
						state[$new]=${state[$who]}
						hold[$new]=${hold[$who]}
						unset state[$who] hold[$who]
						log "transfer the worker state to $new"
						local id
						for id in $(filter_keys assign $who); do
							log "transfer the ownership of assignment $id"
							assign[$id]=$new
						done
					fi
					local id
					for id in $(filter_keys own $who); do
						log "transfer the ownerships of request $id and response $id"
						own[$id]=$new
					done
					local item
					for item in $(vfmt=" %s " filter_keys notify "* $who *"); do
						log "transfer the $item subscription to $new"
						local subscribers=(${notify[$item]})
						erase_from subscribers $who
						subscribers+=($new)
						notify[$item]=${subscribers[@]}
						news[$item-$new]=${news[$item-$who]}
						unset news[$item-$who]
					done
					if contains brokers $who; then
						erase_from brokers $who
						brokers+=($new)
					elif contains brokers $new; then
						contact_brokers $new
					fi
					if contains workers $who; then
						erase_from workers $who
						workers+=($new)
					elif contains workers $new; then
						contact_workers $new
					fi
				fi

			elif [ "$type" == "%" ]; then
				if [[ "$info" == "protocol"* ]]; then
					log "chat system protocol verified successfully"
					log "register node $name on the chat system..."
					echo "name $name"

				elif [[ "$info" == "name"* ]]; then
					log "registered as ${name:=${info:6}} successfully"
					declare registered=$name
					set_capacity ${capacity}
					set_affinity ${affinity:-0}
					observe_overview; observe_status
					log "initialized, state = ${system_status[@]}"
					[[ ${brokers[@]} ]] && contact_brokers ${brokers[@]}
					[[ ${workers[@]} ]] && contact_workers ${workers[@]}

				elif [[ "$info" == "failed protocol"* ]]; then
					log "unsupported protocol; shutdown"
					return 1

				elif [[ "$info" == "failed name"* ]]; then
					unset registered
					log "name $name has been occupied, query online names..."
					echo "who"

				elif [[ "$info" == "failed chat"* ]]; then
					log "failed chat, check online names..."
					echo "who"

				elif [[ "$info" == "who: "* ]]; then
					local online=(${info:5})

					if [[ ! $registered ]]; then
						name=${name:-node}
						while contains online $name; do
							name=${name%-*}-$((${name##*-}+1))
						done
						log "register node $name on the chat system..."
						echo "name $name"
					else
						local item who
						if (( ${#own[@]} )) && [[ ! $keep_unowned_tasks ]]; then
							for who in $(printf "%s\n" "${own[@]}" | sort | uniq); do
								contains online $who && continue
								log "$who logged out silently"
								discard_owned_assets $(filter_keys own $who)
							done
						fi
						if (( ${#assign[@]} )); then
							for who in $(printf "%s\n" "${assign[@]}" | sort | uniq); do
								contains online $who && continue
								log "$who logged out silently"
								discard_workers $who
							done
						fi
						for item in ${!notify[@]}; do
							for who in ${notify[$item]}; do
								contains online $who && continue
								log "$who disconnected silently, unsubscribe $item for $who"
								unsubscribe $item $who
							done
						done
					fi
				fi
			fi

		elif ! [ "$message" ]; then
			local current=$(date +%s%3N)
			local id
			for id in ${!tmdue[@]}; do
				local due=${tmdue[$id]}
				if (( $current > $due )); then
					log "request $id failed due to timeout" \
					    "(due $(date '+%Y-%m-%d %H:%M:%S' -d @${due:0:-3}).${due: -3}), notify ${own[$id]}"
					local code="timeout"
					local output=
					res[$id]=$code:${stdout[$id]}$output
					echo "${own[$id]} << response $id $code {$output}"
					terminate $id
				fi
			done
			local id
			for id in ${!hdue[@]}; do
				local due=${hdue[$id]}
				if (( $current > $due )); then
					queue=($id ${queue[@]})
					log "request $id failed to be assigned" \
					    "(due $(date '+%Y-%m-%d %H:%M:%S' -d @${due:0:-3}).${due: -3}), queue = ($(omit ${queue[@]}))"
					adjust_worker_state ${assign[$id]} -1
					echo "${assign[$id]} << report state requests"
					unset assign[$id] hdue[$id]
				fi
			done

			jobs >/dev/null 2>&1

		else
			handle_extended_input "$message" || log "ignore message: $message"
		fi

		if (( ${#queue[@]} )) && [[ ${state[@]} == *"idle"* ]]; then
			assign_requests
		fi

		local id code output
		while (( ${#pid[@]} )) && fetch_response; do
			if [[ -v cmd[$id] ]] && [[ -v own[$id] ]]; then
				if [ $code != output ]; then
					res[$id]=$code:${stdout[$id]}$output
					[[ -v stdin[$id] ]] && exec {stdin[$id]}>&-
					[[ -v stdout[$id] ]] && stdout[$id]=
					unset pid[$id] assign[$id] tmdue[$id] hdue[$id]
				else
					stdout[$id]+=$output\\n
				fi
				log "complete response $id $code {$output} and forward it to ${own[$id]}"
				echo "${own[$id]} << response $id $code {$output}"
			else
				log "complete orphan response $id $code {$output}"
			fi
		done

		refresh_observations
	done

	log "message input is terminated, chat system is down?"
	return 16
}

confirm_request() {
	local id=$1
	return 0
}

extract_options() {
	opts=()
	local labels="$@" opt val
	for opt in $labels; do
		val="default_$opt"
		[[ ${!val} ]] && opts[$opt]=${!val}
	done
	[[ ! $options ]] && return 0
	options=" $options "
	local regex_option=" (${labels// /|})(=[^ ]*)? "
	while [[ $options =~ $regex_option ]]; do
		options=${options/"$BASH_REMATCH"/ }
		opt=${BASH_REMATCH[1]}
		val=${BASH_REMATCH[2]}
		opts[$opt]=${val:1}
	done
	[[ ! $options ]] && return 0
	options=$(<<< $options xargs)
	[[ ! $options ]]
}

assign_requests() {
	declare -A workers
	local id request with worker pref

	for id in ${queue[@]}; do
		pref=$(prefer_workers $id)
		[[ -v workers[$pref] ]] || workers[$pref]=$(filter "$pref" ${!state[@]})
		[[ ${workers[$pref]} ]] || continue

		workers[$pref]=$(sort_idle_workers ${workers[$pref]})
		worker=(${workers[$pref]})
		[[ $worker ]] || continue

		if [[ $worker != $name ]]; then
			request="$id {${cmd[$id]}}"
			with=
			[[ -v stdin[$id] ]] && with+=" input"
			[[ -v stdout[$id] ]] && with+=" output"
			request+=${with:+ with}${with}

			echo "$worker << request $request"
			adjust_worker_state $worker +1
			assign[$id]=$worker
			hdue[$id]=$(($(date +%s%3N)+${hold_timeout:-1000}*${hold[$worker]}))
			erase_from queue $id
			log "assign request $id to $worker," \
			    "assume that $worker state ${state[$worker]/:/ } (${hold[$worker]} hold)"

		elif execute_request $id; then
			adjust_worker_state $worker +1
			hold[$worker]=$((hold[$worker]-1))
			assign[$id]=$worker
			erase_from queue $id
			log "execute request $id at $worker," \
			    "$worker state ${state[$worker]/:/ }"
			notify_assign_request $id $worker

		else
			log "failed to execute request $id"
		fi
	done
}

prefer_workers() {
	local id=$1
	echo "${prefer[$id]:-${prefer_workers:-*}}"
}

sort_idle_workers() {
	local workers=() worker stat load
	for worker in $@; do
		stat=${state[$worker]}
		[[ $stat == "idle"* ]] || continue
		load=${stat:5}
		load=${load%/*}
		workers[$load]+=" $worker"
	done
	printf "%s\n" ${workers[@]}
}

adjust_worker_state() {
	local worker=$1 adjust=$2 stat load size
	load=${state[$worker]:5}
	load=(${load/\// })
	size=${load[1]}
	load=$((load + adjust))
	(( load < size )) && stat=idle || stat=busy
	state[$worker]=$stat:$load/$size
	hold[$worker]=$((hold[$worker] + adjust))
}

execute_request() {
	local id=$1 output=store
	[[ -v stdout[$id] ]] && output=flush stdout[$id]=
	if [[ ! -v stdin[$id] ]]; then
		execute $id $output >&${res_fd} {res_fd}>&- &
		pid[$id]=$!
		return 0
	elif exec {stdin[$id]}<> <(:); then
		execute $id $output <&${stdin[$id]} >&${res_fd} {stdin[$id]}<&- {res_fd}>&- &
		pid[$id]=$!
		return 0
	else
		return 1
	fi
}

execute() {
	local id=$1 output=${2:-store} code
	if [[ $output == store ]]; then
		output=$(eval "${cmd[$id]}" 2>&1)
		code=$?
		output=$(echo -n "$output" | format_output)
		echo "response $id $code {$output}"
	elif [[ $output == flush ]]; then
		shopt -s lastpipe
		eval "${cmd[$id]}" 2>&1 | while IFS= read -r output; do
			output=$(echo -n "$output" | format_output)
			echo "response $id output {$output}"
		done
		code=${PIPESTATUS[0]}
		if [[ $output ]]; then
			output=$(echo -n "$output" | format_output)
			echo "response $id output {$output}"
		fi
		echo "response $id $code {}"
	else
		echo "response $id -1 {}"
	fi
}

format_output() {
	# drop ASCII terminal color codes then escape '\' '\n' '\t' with '\'
	sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g' | \
		sed -z 's/\\/\\\\/g' | sed -z 's/\t/\\t/g' | sed -z 's/\n/\\n/g' | tr -d '[:cntrl:]'
}

fetch_response() {
	local response
	read -r -t 0 -u ${res_fd} && IFS= read -r -u ${res_fd} response || return $?
	IFS=' ' read -r response id code output <<< $response || return $?
	[ "${output:0:1}${response}${output: -1}" == "{response}" ] || return $?
	output=${output:1:${#output}-2}
	return $?
}

terminate() {
	local id
	for id in $@; do
		if [[ -v pid[$id] ]]; then
			if kill_request $id; then
				log "request $id has been terminated successfully"
			fi
		elif [[ -v assign[$id] ]]; then
			[[ -v hdue[$id] ]] && adjust_worker_state ${assign[$id]} -1
			echo "${assign[$id]} << terminate $id"
			log "forward terminate $id to ${assign[$id]}"
		fi
		[[ -v stdin[$id] ]] && stdin[$id]=
		[[ -v stdout[$id] ]] && stdout[$id]=
		unset pid[$id] assign[$id] tmdue[$id] hdue[$id]
	done
	erase_from queue $@
}

kill_request() {
	local id code=0
	for id in $@; do
		kill ${pid[$id]} $(cmdpidof $id) 2>/dev/null
		code=$(($?|code))
		[[ -v stdin[$id] ]] && exec {stdin[$id]}>&-
		[[ -v stdout[$id] ]] && stdout[$id]=
	done
	return $code
}

cmdpidof() {
	{ pgrep -P $(pgrep -P ${pid[$1]}); } 2>/dev/null
}

app() {
	local app=$1
	[[ $app ]] || return 127
	local app_home="${app_home:-.cache/worker}/$app"
	pushd "$app_home" >/dev/null 2>&1
	if [ $? != 0 ]; then # get app package from repo...
		local app_repo=$(realpath -q "${app_repo:-.}" || echo "${app_repo}")
		mkdir -p "$app_home"
		pushd "$app_home" >/dev/null 2>&1 || return 128
		local fmt
		for fmt in ".tar.xz" ".tar.gz" ".zip" ""; do
			pkg-get "${app_repo}/${app}${fmt}" && break
		done 2>/dev/null
		if [ $? != 0 ]; then # app package not found
			popd >/dev/null 2>&1
			rm -r "$app_home"
			return -1
		elif [ -e setup.sh ]; then
			bash setup.sh 2>&1 >setup.log
		fi
	fi
	local code
	eval "${@:2}"
	code=$?
	popd >/dev/null 2>&1
	return $code
}

pkg-get() {
	local pkg=$1
	[[ $pkg ]] || return 127
	pkg=$(realpath -q "$pkg" || echo "$pkg")
	local tmp=$(mktemp -d -p .)
	pushd $tmp >/dev/null || return 128
	local code=0
	if [[ $pkg != http* ]]; then
		rsync -aq "$pkg" .
	else
		curl -OJRfs "$pkg"
	fi
	code=$(($?|code))
	popd >/dev/null
	if [ $code == 0 ]; then
		unpack $tmp/*
		code=$(($?|code))
	fi
	rm -r $tmp
	return $code
}

unpack() {
	local code=0 pkg
	for pkg in "$@"; do
		case "${pkg,,}" in
		*.tar.xz|*.txz)	tar Jxf "$pkg"; ;;
		*.tar.gz|*.tgz)	tar zxf "$pkg"; ;;
		*.tar)	tar xf "$pkg"; ;;
		*.xz)	xz -kd "$pkg"; ;;
		*.gz)	gzip -kd "$pkg"; ;;
		*.zip)	unzip -q -o "$pkg"; ;;
		*.7z)	7za x -y "$pkg" >/dev/null; ;;
		esac; code=$(($?|code))
	done
	return $code
}

set_capacity() {
	capacity=${1}
}

set_affinity() {
	affinity=$((${1:-$(nproc)}))
	local stat load size
	if (( affinity )); then
		load=${state[$name]:5}
		load=$(( ${load%/*} ))
		(( load < affinity )) && stat=idle || stat=busy
		state[$name]=$stat:$load/$affinity
	elif [[ -v state[$name] ]]; then
		discard_workers $name
	fi
}

observe_overview() {
	local overview_last=${overview[@]}
	lastview=(${overview[@]})

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

observe_idle() {
	[ "$overview" != "$lastview" ] && (( ${overview[3]%+*} < ${overview[1]#*/} ))
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
	if [[ ${notify[idle]} ]] && observe_idle; then
		local notify_stat=$overview
		printf "%s << notify state $notify_stat\n" ${notify[idle]}
		log "state $notify_stat, notify ${notify[idle]}"
	fi
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

discard_owned_assets() {
	local id
	for id in $@; do
		if [[ -v res[$id] ]]; then
			log "discard request $id and response $id"
		else
			log "discard request $id"
		fi
		terminate $id
		unset cmd[$id] own[$id] res[$id] tmout[$id] prefer[$id] stdin[$id] stdout[$id]
	done
}

change_brokers() {
	local current=($1) pending=($2)
	local added=(${pending[@]}) removed=(${current[@]})
	erase_from added ${current[@]}
	erase_from removed ${pending[@]}
	log "confirm brokers change: (${current[@]}) --> (${pending[@]})"
	[[ ${removed[@]} ]] && discard_brokers ${removed[@]}
	[[ ${added[@]} ]] && contact_brokers ${added[@]}
	brokers=(${pending[@]})
}

change_workers() {
	local current=($1) pending=($2)
	local added=(${pending[@]}) removed=(${current[@]})
	erase_from added ${current[@]}
	erase_from removed ${pending[@]}
	log "confirm workers change: (${current[@]}) --> (${pending[@]})"
	[[ ${removed[@]} ]] && discard_workers ${removed[@]}
	[[ ${added[@]} ]] && contact_workers ${added[@]}
	workers=(${pending[@]})
}

contact_brokers() {
	local handshake=($@)
	erase_from handshake ${notify[state]} $name
	if [[ ${handshake[@]} ]]; then
		log "contact ${handshake[@]} for handshake (protocol 0)"
		printf "%s << use protocol 0\n" "${handshake[@]}"
	fi
}

contact_workers() {
	local contact=($@)
	erase_from contact $name
	if [[ ${contact[@]} ]]; then
		log "contact ${contact[@]} for worker state"
		printf "%s << report state\n" "${contact[@]}"
	fi
}

discard_brokers() {
	unsubscribe state "$@"
}

discard_workers() {
	local worker ids id
	for worker in "$@"; do
		unset state[$worker] hold[$worker]
		log "discard the worker state of $worker"
		ids=$(filter_keys assign $worker)
		[[ $ids ]] || continue
		terminate $ids
		queue=($ids ${queue[@]})
		log "revoke assigned request $ids, queue = ($(omit ${queue[@]}))"
	done
}

notify_assign_request() {
	local id=$1 who=$2
	if [[ -v news[assign-${own[$id]}] ]]; then
		echo "${own[$id]} << notify assign request $id to $who"
		log "assigned request $id to $who, notify ${own[$id]}"
	fi
	if [[ -v news[idle-${own[$id]}] ]] && (( ${overview[3]%+*} < ${overview[1]#*/} )); then
		echo "${own[$id]} << notify state idle"
		log "state idle, notify ${own[$id]}"
	fi
}

subscribe() {
	local item=${1:-null} who
	for who in ${@:2}; do
		local subscribers=(${notify[$item]})
		erase_from subscribers $who
		subscribers+=($who)
		notify[$item]=${subscribers[@]}
		news[$item-$who]=subscribe
	done
}

unsubscribe() {
	local item=${1:-null} who
	for who in ${@:2}; do
		local subscribers=(${notify[$item]})
		erase_from subscribers $who
		notify[$item]=${subscribers[@]}
		[[ ${notify[$item]} ]] || unset notify[$item]
		unset news[$item-$who]
	done
}

handle_extended_input() {
	local message=$1
	return 1
}

millisec() {
	[[ $1 =~ ^[0-9]* ]]
	case "${1:${#BASH_REMATCH}}" in
		h*) echo $((BASH_REMATCH*3600000)); ;;
		ms) echo $((BASH_REMATCH)); ;;
		m*) echo $((BASH_REMATCH*60000)); ;;
		*)  echo $((BASH_REMATCH*1000)); ;;
	esac
}

init_system_io() {
	local endpoint=$(printf "%s\n" "$addr:$port" "${configs[@]}" | grep -E "^([^:=]+):([0-9]+)$")
	local addr port; IFS=: read -r addr port <<< $endpoint
	if [[ $addr ]] && [[ $port ]]; then
		log "connect to chat system at $addr:$port..."
		while ! { exec {tcp_fd}<>/dev/tcp/$addr/$port; } 2>/dev/null; do
			log "failed to connect $addr:$port, host down?"
			local io_count=${io_count:-0}
			if (( $((++io_count)) >= ${max_io_count:-65536} )); then
				log "max number of connections is reached"
				return 15
			fi
			log "wait ${wait_for_conn:-60}s before the next attempt..."
			sleep ${wait_for_conn:-60}
		done
		log "connected to chat system successfully"
		if ! { exec 0<&$tcp_fd 1>&$tcp_fd {tcp_fd}>&-; } 2>/dev/null; then
			log "failed to redirect input/output to chat system"
			return 14
		fi
	fi
	if [[ ${res_fd} ]] || { exec {res_fd}<> <(:); } 2>/dev/null; then
		log "initialized response pipe successfully"
	else
		log "failed to initialize response pipe"
		return 13
	fi
	return 0
}

cleanup() { :; }

args_of() {
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

omit() {
	if (( "$#" <= ${max_printable_list_size:-10} )); then
		echo "$@"
	else
		local num_show=${num_print_when_omitted:-6}
		echo "${@:1:$((num_show/2))}" "...[$(($#-num_show))]..." "${@:$#-$((num_show/2-1))}"
	fi
}

filter() {
	local item
	for item in "${@:2}"; do [[ $item == $1 ]] && echo "$item"; done
}

filter_keys() {
	local map="$1"
	local key_match="*" val_match="$2"
	[ "$3" ] && local key_match="$2" val_match="$3"
	eval local keys=('${!'$map'[@]}')
	for key in ${keys[@]}; do
		[[ $key == $key_match ]] || continue
		local show=$map[$key]
		local val=$(printf "${vfmt:-%s}" "${!show}")
		[[ $val == $val_match ]] && echo $key
	done
}

contains() {
	local show=${1:-_}[@]
	[[ " ${!show} " == *" ${2} "* ]]
}

erase_from() {
	local -n from=$1
	local patt=" ${@:2} "
	local save item
	for item in ${from[@]}; do
		[[ $patt == *" $item "* ]] || save+="$item "
	done
	eval "${1:-_}=($save)"
}

retain_from() {
	local -n from=$1
	local patt=" ${@:2} "
	local save item
	for item in ${from[@]}; do
		[[ $patt == *" $item "* ]] && save+="$item "
	done
	eval "${1:-_}=($save)"
}

override() {
	eval "local level=${1}_override_level"
	eval "$(echo "${1}_override_$((level))()"; declare -f ${1} | tail -n +2)" 2>/dev/null
	eval "${1}_override_level=$((level+1))"
}

invoke_overridden() {
	eval "local level=${invoke_level:-${1}_override_level}"
	(( level-- )) || return 255
	invoke_level=$level ${1}_override_$level "${@:2}"
}

xargs_eval() {
	local item
	local read="read -r"
	case "$1" in
	-d)  read+=" -d\"${2}\""; shift 2; ;;
	-d*) read+=" -d\"${1:2}\""; shift; ;;
	esac
	local exec="${@:-echo}"
	[[ $exec == *"{}"* ]] || exec+=" {}"
	exec=${exec//{\}/\$item}
	while eval $read item; do eval "$exec"; done
	[[ $item ]] && eval "$exec"
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

envinfo() {
	# host name
	echo "Host: $(hostname)"
	# OS name and version
	local osinfo=$(uname -o 2>/dev/null | sed "s|GNU/||")
	osinfo+=" $(uname -r | sed -E 's/[^0-9.]+.+$//g')"
	if [[ $OSTYPE =~ cygwin|msys ]]; then
		local ver=($(cmd /c "ver" 2>/dev/null | tr "[\r\n]" " "))
		[[ ${ver[@]} ]] && osinfo+=" (Windows ${ver[-1]})"
	fi
	echo "OS: $osinfo"
	# CPU model
	local cpuinfo=$(grep -m1 "model name" /proc/cpuinfo | sed -E 's/.+:|\(\S+\)|CPU|[0-9]+-Core.+|@.+//g' | xargs)
	local nodes=$(grep "physical id" /proc/cpuinfo | grep -o [0-9] | sort -n | uniq -c)
	local nproc=() num id
	while read -r num id; do nproc[$id]=$num; done <<< $nodes
	if (( ${#nproc[@]} > 1 )) || [[ ! ${nproc[0]} ]]; then
		for id in ${!nproc[@]}; do echo "CPU $id: $cpuinfo (${nproc[$id]}x)"; done
	else
		echo "CPU: $cpuinfo (${nproc:-$(nproc --all)}x)"
	fi
	# CPU affinity
	if [ $(nproc) != $(nproc --all) ]; then
		echo "CPU Affinity: $(taskset -pc $$ | cut -d' ' -f6) ($(nproc)x)"
	fi
	# GPU model
	local nvsmi=$(nvidia-smi -L 2>/dev/null | sed -E "s/ \(UUID:.+$//g" | grep "^GPU")
	[[ $(wc -l <<< $nvsmi) == 1 ]] && nvsmi=${nvsmi/GPU 0:/GPU:}
	xargs -rL1 <<< $nvsmi
	# memory info
	local size=($(head -n1 /proc/meminfo))
	size=$((${size[1]}0/1024/1024))
	size=$((size/10)).$((size%10))
	echo "RAM: $(printf %.1fG $size)"
}

log() {
	name=${name:-$(basename "${0%.sh}")}
	logfile=${logfile:-${name}_$(date '+%Y%m%d_%H%M%S_%3N').log}
	if exec 3>> "$logfile" && flock -xn 3; then
		trap 'code=$?; flock -u 3; exit $code' EXIT
		exec 2> >(trap '' INT TERM; exec tee /dev/fd/2 >&3)
	fi
	trap 'log "${name:-node} has been interrupted"; exit 64' INT
	trap 'log "${name:-node} has been terminated"; exit 64' TERM
	trap 'code=$?; cleanup; log "${name:-node} is terminated"; exit $code' EXIT

	log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" >&2; }
	log "$@"
}

#### script main ####
if [ "$0" == "$BASH_SOURCE" ]; then
	main "$@"
fi
