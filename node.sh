#!/bin/bash

main() {
	declare "$@" >/dev/null 2>&1
	declare configs=(name ${@%%=*} logfile)

	declare name=${name-$(name)}
	declare brokers=(${brokers//:/ } ${broker//:/ })
	declare workers=(${workers//:/ } ${worker//:/ })
	declare capacity=${capacity-+65536}
	declare affinity=${affinity-$(nproc)}
	declare plugins=${plugins}
	declare logfile=${logfile}

	log "chat::node version 2022-11-23 (protocol 0)"
	args_of ${configs[@]} | xargs_eval log "option:"
	envinfo | xargs_eval log "platform"
	foreach source ${plugins//:/ } >/dev/null
	init_configs ${mode:-$(basename "$0" .sh)}

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
	declare exit_code

	while [[ ! $exit_code ]]; do
		init_system_io && session "$@"
	done
	return $exit_code
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

	declare registered

	log "verify chat system protocol 0..."
	echo "protocol 0"

	local message from info
	while input message; do
		from=${message%% *}
		info=${message#* >> }

		if handle_${info%% *}_input "$info" "$from" || handle_extended_input "$info" "$from"; then
			complete_input
		else
			[[ $exit_code ]] && return $exit_code
			log "ignore message: $message"
		fi
	done

	log "message input terminated unexpectedly"
	return 16
}

handle_request_input() { # ^request (([0-9]+) )?(\{(.*)\}( with( ([^{}]+)| ?))?|(.+))$
	local regex_request="^request (([0-9]+) )?(\{(.*)\}( with( ([^{}]+)| ?))?|(.+))$"
	[[ $1 =~ $regex_request ]] || return 1

	local owner=$2
	local id=${BASH_REMATCH[2]}
	local command=${BASH_REMATCH[4]:-${BASH_REMATCH[8]}}
	local options=${BASH_REMATCH[7]}

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
		optionalize_request $id

		if confirm_request $id && initialize_request $id; then
			id_next=$((id+1))
			echo "$owner << accept request $request"
			log "accept request $id {$command} ${options:+with $options}from $owner, queue = ($(omit ${queue[@]}))"
		else
			erase_from queue $id
			unset own[$id] cmd[$id] tmout[$id] tmdue[$id] prefer[$id] stdin[$id] stdout[$id]
			echo "$owner << reject request $request"
			log "reject request $id {$command} ${options:+with $options}from $owner due to policy"
		fi

	elif [[ -v stdin[$id] ]] && [[ $command == "input "* ]]; then
		local input=${command:6}
		[ "${input:0:1}${input: -1}" == "{}" ] && input=${input:1:-1}

		if [[ ${own[$id]} == $owner ]] && [[ -v assign[$id] ]] && [[ ! -v hdue[$id] ]]; then
			if [[ ${assign[$id]} != $name ]]; then
				echo "${assign[$id]} << request $id input {$input}"
				echo "$owner << confirm request $id"
				log "accept request $id input {$input} from $owner and forward it to ${assign[$id]}"
			else
				echo "$input" >&${stdin[$id]}
				echo "$owner << confirm request $id"
				log "accept request $id input {$input} from $owner and input it into $id"
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

	return 0
}

handle_response_input() { # ^response (\S+) (\S+) \{(.*)\}$
	local what id code output worker=$2
	IFS=' ' read -r what id code output <<< $1 || return 1
	[ "${output:0:1}$what${output: -1}" == "{response}" ] || return 1
	output=${output:1:-1}

	if [[ -v cmd[$id] ]] && [[ ${assign[$id]} == $worker ]]; then
		if [ $code != output ]; then
			res[$id]=$code:${stdout[$id]}$output
			adjust_worker_state $worker -1
			[[ -v stdout[$id] ]] && stdout[$id]=
			unset assign[$id] tmdue[$id]
			echo "$worker << accept response $id"
		else
			stdout[$id]+=$output\\n
			echo "$worker << confirm response $id"
		fi
		if [[ -v hdue[$id] ]]; then
			hold[$worker]=$((hold[$worker]-1))
			log "confirm that $worker acquiesced accepted request $id"
			unset hdue[$id]
			notify_assign_request $id $worker
		fi
		echo "${own[$id]} << response $id $code {$output}"
		log "accept response $id $code {$output} from $worker and forward it to ${own[$id]}," \
		    "assume that $worker state ${state[$worker]/:/ }"

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

	return 0
}

handle_terminate_input() { # ^terminate (.+)$
	local id=${1#* }
	local who=$2

	local ids=()
	if [[ ${own[$id]} == $who ]] && [[ ! -v res[$id] ]]; then
		ids=($id)
	elif [[ $id == *[^0-9]* ]]; then
		ids=($(<<<$id xargs_eval -d' ' "filter_keys own \"{}\" $who" | sort -nu))
		erase_from ids ${!res[@]}
	fi
	if [[ ${ids[@]} ]]; then
		printf "$who << accept terminate %d\n" ${ids[@]}
		log "accept terminate ${ids[@]} from $who"
		for id in ${ids[@]}; do
			terminate $id
			unset cmd[$id] own[$id] tmout[$id] tmdue[$id] prefer[$id] stdin[$id] stdout[$id]
		done

	elif [ "${own[$id]}" ] && [[ ! -v res[$id] ]]; then
		echo "$who << reject terminate $id"
		log "reject terminate $id from $who since it is owned by ${own[$id]}"
	else
		echo "$who << reject terminate $id"
		log "reject terminate $id from $who since no such request"
	fi

	return 0
}

handle_notify_input() { # ^notify state (idle|busy|full) (\S+)/(\S+)( \((.*)\))?$
	local label what stat load size owned worker=$2
	IFS=' ' read -r label what stat load size owned <<< "${1/\// }" || return 1

	[ "$label $what" == "notify state" ] || return 1

	if [[ $stat == idle || $stat == busy || $stat == full ]] && [[ ${load:-x}${size:-x} != *[^0-9]* ]]; then
		echo "$worker << confirm state $stat $load/$size"

		if (( ! hold[$worker] )); then
			log "confirm that $worker state $stat $load/$size"
			state[$worker]=$stat:$load/$size
		else
			load=$((load + hold[$worker]))
			(( load < size )) && stat="idle" || stat="busy"
			log "confirm that $worker state $stat $load/$size (${hold[$worker]} hold)"
			state[$worker]=$stat:$load/$size
		fi

		if [[ $owned ]]; then
			local ids=($(filter_keys assign $worker))
			erase_from ids ${owned:1:-1} ${!hdue[@]}
			if [[ ${ids[@]} ]]; then
				queue=(${ids[@]} ${queue[@]})
				log "confirm that $worker disowned request ${ids[@]}, queue = ($(omit ${queue[@]}))"
			fi
		fi

	else
		log "ignore incorrect state $stat $load/$size"
		return 1
	fi

	return 0
}

handle_confirm_input() { # ^(accept|reject|confirm) (\S+) (.+)$
	local confirm what option
	IFS=' ' read -r confirm what option <<< $1 || return 1
	local id=$option
	local who=$2

	if [ "$what" == "state" ]; then
		log "confirm that $who ${confirm}ed state $option"

	elif [ "$what" == "request" ]; then
		local ids=()
		if [[ ${assign[$id]} == $who ]] && [[ -v hdue[$id] ]]; then
			ids=($id)
		elif [[ $id == *[^0-9]* ]]; then
			ids=($(<<<$id xargs_eval -d' ' "filter_keys assign \"{}\" $who" | sort -nu))
			retain_from ids ${!hdue[@]}
		fi
		if [[ ${ids[@]} ]] && [ "$confirm" == "accept" ]; then
			hold[$who]=$((hold[$who]-${#ids[@]}))
			log "confirm that $who ${confirm}ed $what ${ids[@]}"
			for id in ${ids[@]}; do
				unset hdue[$id]
				notify_assign_request $id $who
			done
		elif [[ ${ids[@]} ]] && [ "$confirm" == "reject" ]; then
			for id in ${ids[@]}; do
				unset hdue[$id] assign[$id]
			done
			adjust_worker_state $who -${#ids[@]}
			hold[$who]=$((hold[$who]-${#ids[@]}))
			queue=(${ids[@]} ${queue[@]})
			log "confirm that $who ${confirm}ed $what ${ids[@]}, queue = ($(omit ${queue[@]}))"
		elif [[ -v stdin[$id] ]] && [ "$confirm" == "confirm" ]; then
			log "confirm that $who ${confirm}ed $what $id input"
		else
			log "ignore that $who ${confirm}ed $what $id since no such $what"
		fi

	elif [ "$what" == "response" ]; then
		local ids=()
		if [[ ${own[$id]} == $who ]] && [[ -v res[$id] ]]; then
			ids=($id)
		elif [[ $id == *[^0-9]* ]]; then
			ids=($(<<<$id xargs_eval -d' ' "filter_keys own \"{}\" $who" | sort -nu))
			retain_from ids ${!res[@]}
		fi
		if [[ ${ids[@]} ]] && [ "$confirm" == "accept" ]; then
			for id in ${ids[@]}; do
				unset res[$id] cmd[$id] own[$id] tmout[$id] prefer[$id] stdin[$id] stdout[$id]
			done
			log "confirm that $who ${confirm}ed $what ${ids[@]}"
		elif [[ ${ids[@]} ]] && [ "$confirm" == "reject" ]; then
			queue+=(${ids[@]})
			for id in ${ids[@]}; do
				if confirm_request $id && initialize_request $id; then
					echo "$who << accept request $id"
				else
					echo "$who << reject request $id"
					erase_from queue $id
				fi
			done
			log "confirm that $who ${confirm}ed $what ${ids[@]}, queue = ($(omit ${queue[@]}))"
		elif [[ -v stdout[$id] ]] && [ "$confirm" == "confirm" ]; then
			log "confirm that $who ${confirm}ed $what $id output"
		else
			log "ignore that $who ${confirm}ed $what $id since no such $what"
		fi

	elif [ "$what" == "terminate" ]; then
		local ids=() ida=()
		if [[ ! -v assign[$id] ]]; then
			ids=($id)
		elif [[ ${assign[$id]} == $who ]]; then
			ids=($id)
			ida=($id)
		elif [[ $id == *[^0-9]* ]]; then
			ids=($(<<<$id xargs_eval -d' ' "filter_keys assign \"{}\" $who" | sort -nu))
			ida=(${ids[@]})
		fi
		if [[ ${ida[@]} ]] && ([ "$confirm" == "accept" ] || [ "$confirm" == "confirm" ]); then
			for id in ${ida[@]}; do unset assign[$id]; done
			queue=(${ida[@]} ${queue[@]})
			log "confirm that $who ${confirm}ed $what ${ids[@]}, queue = ($(omit ${queue[@]}))"
		elif [[ ${ids[@]} ]]; then
			log "confirm that $who ${confirm}ed $what ${ids[@]}"
		else
			log "ignore that $who ${confirm}ed $what $id since no such $what"
		fi

	elif [ "$what" == "protocol" ]; then
		if [ "$confirm" == "accept" ]; then
			log "handshake with $who successfully"
			subscribe state $who
			log "subscribed state for new broker $who"
			observe_state
			echo "$who << notify state ${system_state[@]}"
		elif [ "$confirm" == "reject" ]; then
			log "handshake failed, unsupported protocol; shutdown"
			exit_code=2
			return 255
		fi

	elif [ "$what" == "restart" ] || [ "$what" == "contact" ]|| [ "$what" == "discard" ]; then
		log "confirm that $who ${confirm}ed ${what}${option:+ $option}"

	else
		return 1
	fi

	return 0
}

handle_accept_input() {
	handle_confirm_input "$@"
}

handle_reject_input() {
	handle_confirm_input "$@"
}

handle_query_input() { # ^query (.+)$
	local options=${1#* }
	local who=$2

	if [ "$options" == "protocol" ] || [ "$options" == "version" ]; then
		echo "$who << protocol 0 version 2022-11-23"
		log "accept query protocol from $who"

	elif [ "$options" == "overview" ]; then
		echo "$who << overview = ${overview[@]}"
		log "accept query overview from $who, overview = ${overview[@]}"

	elif [ "$options" == "capacity" ] || [ "$options" == "affinity" ]; then
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
	{
		local assignment=() id
		for id in ${!assign[@]}; do
			[ "${own[$id]}" == "$who" ] && assignment+=("[$id]=${assign[$id]}")
		done
		echo "$who << assign = (${assignment[@]})"
		log "accept query assign from $who, assign = ($(omit ${assignment[@]}))"
	} &
	elif [[ "$options" =~ ^(broker)s?(.*)$ ]]; then
	{
		local query=() broker
		query=(${BASH_REMATCH[2]:-$(printf "%s\n" ${notify[state]} | sort)})
		retain_from query ${notify[state]}
		echo "$who << brokers = (${query[@]})"
		for broker in ${query[@]}; do
			local owned=($(filter_keys own $broker))
			echo "$who << # $broker ${#owned[@]} owned"
		done
		log "accept query brokers from $who, brokers = ($(omit ${query[@]}))"
	} &
	elif [[ "$options" =~ ^(worker)s?(.*)$ ]]; then
	{
		local query=() worker
		query=(${BASH_REMATCH[2]:-$(printf "%s\n" ${!state[@]} | sort)})
		retain_from workers ${!state[@]}
		echo "$who << workers = (${query[@]})"
		for worker in ${workers[@]}; do
			local assigned=($(filter_keys assign $worker))
			echo "$who << # $worker ${state[$worker]} ${#assigned[@]} assigned"
		done
		log "accept query workers from $who, workers = ($(omit ${query[@]}))"
	} &
	elif [[ "$options" =~ ^(job|task)s?(.*)$ ]] ; then
	{
		local ids=() id
		ids=(${BASH_REMATCH[2]:-$(printf "%s\n" ${!cmd[@]} | sort -n)})
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
	} &
	elif [[ "$options" =~ ^(request)s?(.*)$ ]] ; then
	{
		local ids=() id
		ids=(${BASH_REMATCH[2]:-$(printf "%s\n" ${!cmd[@]} | sort -n)})
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
	} &
	elif [[ "$options" =~ ^(response|result)s?(.*)$ ]] ; then
	{
		local ids=() id
		ids=(${BASH_REMATCH[2]:-$(printf "%s\n" ${!res[@]} | sort -n)})
		retain_from ids $(filter_keys own $who)
		retain_from ids ${!res[@]}
		echo "$who << responses = (${ids[@]})"
		for id in ${ids[@]}; do
			echo "$who << # response $id ${res[$id]%%:*} {${res[$id]#*:}}"
		done
		log "accept query responses from $who, responses = ($(omit ${ids[@]}))"
	} &
	elif [[ "$options" =~ ^(option|variable|argument)s?(.*)$ ]] ; then
	{
		local vars=() args=()
		args_of ${BASH_REMATCH[2]:-${configs[@]}} >/dev/null
		echo "$who << options = (${vars[@]})"
		[[ ${args[@]} ]] && printf "$who << # %s\n" "${args[@]}"
		log "accept query options from $who, options = ($(omit ${vars[@]}))"
	} &
	elif [ "$options" == "envinfo" ]; then
	{
		local envinfo=$(envinfo)
		local envitem=$(cut -d: -f1 <<< "$envinfo" | xargs)
		echo "$who << envinfo = ($envitem)"
		<<< $envinfo xargs -r -d'\n' -L1 echo "$who << #"
		log "accept query envinfo from $who, envinfo = ($envitem)"
	} &
	else
		return 1
	fi

	return 0
}

handle_report_input() { # ^report (.+)$
	local options=${1#* }
	local who=$2

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
	{
		local ids=() id
		ids=(${BASH_REMATCH[2]:-$(printf "%s\n" ${!res[@]} | sort -n)})
		retain_from ids $(filter_keys own $who)
		retain_from ids ${!res[@]}
		log "accept report responses from $who, responses = ($(omit ${ids[@]}))"
		for id in ${ids[@]}; do
			echo "$who << response $id ${res[$id]%%:*} {${res[$id]#*:}}"
		done
	} &
	else
		return 1
	fi

	return 0
}

handle_subscribe_input() { # ^(subscribe|unsubscribe) (.+)$
	local command=${1%% *}
	local options=${1#* }
	local who=$2

	if [ "$command" == "subscribe" ]; then
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

	else
		return 1
	fi

	return 0
}

handle_unsubscribe_input() {
	handle_subscribe_input "$@"
}

handle_set_input() { # ^(set|unset) (.+)$
	local command=${1%% *}
	local options=${1#* }
	local who=$2

	if [ "$command" == "set" ]; then
		local var=(${options/=/ })
		local val=${options:${#var}+1}

		local confirm
		set_config $var "$val" && confirm="accept" || confirm="reject"
		echo "$who << $confirm set ${var}${val:+=${val}}"
		log "$confirm set ${var}${val:+=\"${val}\"} from $who"

	elif [ "$command" == "unset" ]; then
		local var=(${options/=/ })

		local confirm
		unset_config $var && confirm="accept" || confirm="reject"
		echo "$who << $confirm unset $var"
		log "$confirm unset $var from $who"

	else
		return 1
	fi

	return 0
}

handle_unset_input() {
	handle_set_input "$@"
}

handle_use_input() { # ^use (.+)$
	local options=${1#* }
	local who=$2

	if [[ "$options" == "protocol "* ]]; then
		local protocol=${options:9}
		if [ "$protocol" == "0" ]; then
			echo "$who << accept protocol $protocol"
			log "accept use protocol $protocol from $who"
		else
			echo "$who << reject protocol $protocol"
			log "reject use protocol $protocol from $who, unsupported protocol"
		fi
	else
		return 1
	fi

	return 0
}

handle_operate_input() { # ^operate (.+)$
	local options=${1#* }
	local who=$2

	if [[ "$options" =~ ^(shutdown|restart)(\ (.+))?$ ]]; then
		local type=${BASH_REMATCH[1]}
		local patt=${BASH_REMATCH[3]:-$name}
		local matches=($(<<<$patt xargs_eval -d' ' "filter \"{}\" ${!state[@]} $name" | sort -u))

		echo "$who << confirm $type ${matches[@]}"
		log "accept operate $type on ${matches[@]} from $who"

		local clients=(${matches[@]})
		erase_from clients $name
		[[ ${clients[@]} ]] && printf "%s << operate $type\n" ${clients[@]}

		if [[ " ${matches[@]} " == *" $name "* ]]; then
			if [ "$type" == "shutdown" ]; then
				log "$(name) is shutting down..."
				prepare_shutdown
				exit_code=${exit_code:-0}
				return 255

			elif [ "$type" == "restart" ]; then
				log "$(name) is restarting..."
				prepare_restart
				local vars=() args=()
				args_of ${configs[@]} >/dev/null
				exec $0 "${args[@]}"
			fi
		fi

	elif [[ "$options" =~ ^(contact|discard|forward)\ (brokers?|workers?)(\ (.+))?$ ]]; then
		local type=${BASH_REMATCH[1]}
		local what=${BASH_REMATCH[2]}
		local option=${BASH_REMATCH[4]:-"*"}

		if [ "$type" == "contact" ]; then
			log "accept operate $type $what $option from $who"
			echo "$who << confirm $type $what $option"
			foreach contact_${what%s} "$option"

		elif [ "$type" == "discard" ]; then
			local clients=()
			log "accept operate $type $what $option from $who"
			[[ $what == broker* ]] && clients+=($(<<<$option xargs_eval -d' ' "filter \"{}\" ${notify[state]}"))
			[[ $what == worker* ]] && clients+=($(<<<$option xargs_eval -d' ' "filter \"{}\" ${!state[@]}"))
			clients=($(printf "%s\n" ${clients[@]} | sort -u))
			if [[ ${clients[@]} ]]; then
				echo "$who << confirm $type $what ${clients[@]}"
				foreach discard_${what%s} ${clients[@]}
			fi

		elif [ "$type" == "forward" ]; then
			local clients=()
			[[ $what == broker* ]] && clients+=(${notify[state]})
			[[ $what == worker* ]] && clients+=(${!state[@]})
			clients=($(printf "%s\n" ${clients[@]} | sort -u))
			if [[ ${clients[@]} ]]; then
				echo "$who << confirm $type $what $option, ${what%s}s = ${clients[@]}"
				for who in ${clients[@]}; do echo "$who << $option"; done
			fi
		fi

	elif [[ "$options" == "plugin "* ]] || [[ "$options" == "source "* ]]; then
		local what=${options:0:6}
		local plug=${options:7}
		log "accept operate $what $plug from $who"
		echo "$who << confirm $what $plug"
		source $plug >/dev/null
		if [[ $what == "plugin" ]] && [[ :$plugins: != *:$plug:* ]]; then
			plugins+=${plugins:+:}$plug
			log "confirm set plugins=\"$plugins\""
			contains configs plugins || configs+=(plugins)
		fi

	elif [[ "$options" == "shell "* ]]; then
		handle_shell_input "$options" "$who"

	elif [[ "$options" == "output "* ]]; then
		local output=${options:7}
		echo "$output"
		log "accept operate output \"$output\" from $who"
		echo "$who << confirm output $output"

	else
		return 1
	fi

	return 0
}

handle_shell_input() { # ^shell (.+)$
	local shell=${1#shell }
	local who=$2

	[[ ${shell:0:1}${shell: -1} == {} ]] && shell=${shell:1:-1}
	echo "$who << accept execute shell {$shell}"
	log "accept execute shell {$shell} from $who"

	if [[ $shell =~ ([\&\ ]+)$ ]]; then
		shell=${shell%$BASH_REMATCH}
		operate_eval "$shell" "$who" &
	else
		operate_eval "$shell" "$who"
	fi

	return 0
}


handle_chat_notification() { # ^(.+)$
	local info=$1
	local type=$2

	[ "$type" == "#" ] || return 1

	if [[ $info == "login: "* ]]; then
		local who=${info:7}
		node_login $who

	elif [[ $info == "logout: "* ]]; then
		local who=${info:8}
		node_logout $who

	elif [[ $info == "name: "*" becomes "* ]]; then
		local who=${info:6}; who=${who%% *}
		local new=${info##* }
		node_rename $who $new

	else
		return 1
	fi

	return 0
}

handle_chat_operation() { # ^(.+)$
	local info=$1
	local type=$2

	[ "$type" == "%" ] || return 1

	if [[ "$info" == "protocol: "* ]]; then
		log "chat system protocol verified successfully"
		log "register node${name:+ $name} on the chat system..."
		echo "name${name:+ $name}"

	elif [[ "$info" == "name: "* ]]; then
		name=${info:6}
		log "registered as $name successfully"
		if [[ ! $registered ]]; then
			init_register $name
			foreach contact_broker ${brokers[@]}
			foreach contact_worker ${workers[@]}
		fi

	elif [[ "$info" == "who: "* ]]; then
		local online=(${info:5})

		if [[ ! $registered ]]; then
			name=$(name)
			while contains online $name; do name=${name%-*}-$((${name##*-}+1)); done
			log "register node${name:+ $name} on the chat system..."
			echo "name${name:+ $name}"
		fi

		local unexpectedly=($(printf "%s\n" ${own[@]} ${!state[@]} $(<<<${!news[@]} sed -E "s/\S+-//g") | sort -u))
		erase_from unexpectedly ${online[@]}

		local who
		for who in $(printf "%s\n" ${unexpectedly[@]} | sort -u); do
			log "$who disconnected unexpectedly"
			node_logout $who
		done

	elif [[ "$info" == "failed name"* ]]; then
		registered=
		log "name${name:+ $name} has been occupied, query online names..."
		echo "who"

	elif [[ "$info" == "failed chat"* ]]; then
		log "failed chat, check online names..."
		echo "who"

	elif [[ "$info" == "failed protocol"* ]]; then
		log "unsupported protocol; shutdown"
		exit_code=1
		return 255

	else
		return 1
	fi

	return 0
}

handle_noinput() {
	local current=$(date +%s%3N)
	check_request_timeout $current
	check_hold_timeout $current
	jobs >/dev/null 2>&1
	return 0
}

eval 'handle_#_input() {
	handle_chat_notification "${1:2}" "$2"
}'

eval 'handle_%_input() {
	handle_chat_operation "${1:2}" "$2"
}'

eval 'handle__input() {
	handle_noinput
}'

handle_extended_input() {
	local info=$1
	local from=$2
	return 1
}

complete_input() {
	if (( ${#queue[@]} )) && [[ ${state[@]} == *"idle"* ]]; then
		assign_requests
	fi
	if (( ${#pid[@]} )); then
		fetch_responses
	fi
	refresh_observations
}

confirm_request() {
	local id=$1
	return 0
}

optionalize_request() {
	local id=$1
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
	return 0
}

initialize_request() {
	local id=$1
	unset res[$id]
	[[ -v stdin[$id] ]] && stdin[$id]=
	[[ -v stdout[$id] ]] && stdout[$id]=
	[[ -v tmout[$id] ]] && tmdue[$id]=$(($(date +%s%3N)+tmout[$id]))
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
			hold[$worker]=$((hold[$worker]+1))
			assign[$id]=$worker
			hdue[$id]=$(($(date +%s%3N)+${hold_timeout:-1000}*${hold[$worker]}))
			erase_from queue $id
			log "assign request $id to $worker," \
			    "assume that $worker state ${state[$worker]/:/ } (${hold[$worker]} hold)"

		elif execute_request $id; then
			adjust_worker_state $worker +1
			assign[$id]=$worker
			erase_from queue $id
			log "execute request $id at $worker," \
			    "verify that $worker state ${state[$worker]/:/ }"
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
	local worker=${1?} adjust=${2?} stat load size
	stat=${state[$worker]:5}
	load=${stat%/*}
	size=${stat#*/}
	load=$((load + adjust))
	(( load < size )) && stat=idle || stat=busy
	state[$worker]=$stat:$load/$size
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

fetch_responses() {
	local id code output
	while (( ${#pid[@]} )) && fetch_response; do
		if [[ -v cmd[$id] ]] && [[ -v own[$id] ]]; then
			if [ $code != output ]; then
				res[$id]=$code:${stdout[$id]}$output
				[[ -v stdin[$id] ]] && exec {stdin[$id]}>&- && stdin[$id]=
				[[ -v stdout[$id] ]] && stdout[$id]=
				unset pid[$id] assign[$id] tmdue[$id]
				adjust_worker_state $name -1
			else
				stdout[$id]+=$output\\n
			fi
			log "complete response $id $code {$output} and forward it to ${own[$id]}," \
			    "verify that $name state ${state[$name]/:/ }"
			echo "${own[$id]} << response $id $code {$output}"
		else
			log "complete orphan response $id $code {$output}"
		fi
	done
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
	local id=$1
	if [[ -v pid[$id] ]]; then
		[[ -v state[$name] ]] && adjust_worker_state $name -1
		kill_request $id
		log "request $id has been terminated," \
		    "verify that $name state ${state[$name]/:/ }"
		unset assign[$id] pid[$id]
		return 0

	elif [[ -v assign[$id] ]]; then
		local worker=${assign[$id]}
		if [[ -v state[$worker] ]]; then
			adjust_worker_state $worker -1
			echo "$worker << terminate $id"
			log "forward terminate $id to $worker," \
			    "assume that $worker state ${state[$worker]/:/ }"
		fi
		if [[ -v hdue[$id] ]] && [[ -v hold[$worker] ]]; then
			hold[$worker]=$((hold[$worker]-1))
		fi
		unset assign[$id] hdue[$id]
		return 0

	elif contains queue $id; then
		erase_from queue $id
		log "request $id has been dequeued, queue = ($(omit ${queue[@]}))"
		return 0

	else
		return 1
	fi
}

kill_request() {
	local id=${1:-x} code
	{	kill ${pid[$id]} $(pgrep -P $(pgrep -P ${pid[$id]}));
		code=$?
	} 2>/dev/null
	unset pid[$id]
	[[ -v stdin[$id] ]] && exec {stdin[$id]}>&-
	return $code
}

check_request_timeout() {
	local current=$1 id
	for id in ${!tmdue[@]}; do
		if (( current > tmdue[$id] )); then
			local due=${tmdue[$id]}
			due=$(date '+%Y-%m-%d %H:%M:%S' -d @${due:0:-3}).${due: -3}
			local code="timeout" output=
			res[$id]=$code:${stdout[$id]}$output
			terminate $id
			unset tmdue[$id]
			log "request $id timeout at $due, notify ${own[$id]}"
			echo "${own[$id]} << response $id $code {$output}"
		fi
	done
}

check_hold_timeout() {
	local current=$1 id
	for id in ${!hdue[@]}; do
		if (( current > hdue[$id] )); then
			local due=${hdue[$id]}
			due=$(date '+%Y-%m-%d %H:%M:%S' -d @${due:0:-3}).${due: -3}
			local worker=${assign[$id]}
			adjust_worker_state $worker -1
			hold[$worker]=$((hold[$worker]-1))
			queue=($id ${queue[@]})
			unset assign[$id] hdue[$id]
			log "request $id hold expires at $due, queue = ($(omit ${queue[@]}))," \
			    "assume that $worker state ${state[$worker]/:/ }"
			echo "$worker << report state requests"
		fi
	done
}

operate_eval() {
	local shell=$1 who=$2
	local output code lines
	output=$(eval "$shell" 2>&1)
	code=$?
	lines=$((${#output} ? $(<<<$output wc -l) : 0))
	echo "$who << result shell {$shell} return $code ($lines)"
	echo -n "$output" | xargs -r -d'\n' -L1 echo "$who << #"
}

set_config() {
	local var=$1
	local val=$2
	local show_val="$var[@]"
	local val_old="${!show_val}"
	eval $var="\"$val\""
	contains configs $var || configs+=($var)

	if [ "$var" == "name" ]; then
		log "node name has been changed, register${name:+ $name} on the chat system..."
		echo "name${name:+ $name}"
	elif [ "$var" == "brokers" ] || [ "$var" == "broker" ]; then
		brokers=${!var}; brokers=(${brokers//:/ })
		change_brokers "$val_old" "${brokers[@]}"
		erase_from configs brokers broker; contains configs brokers || configs+=(brokers)
	elif [ "$var" == "workers" ] || [ "$var" == "worker" ]; then
		workers=${!var}; workers=(${workers//:/ })
		change_workers "$val_old" "${workers[@]}"
		erase_from configs workers worker; contains configs workers || configs+=(workers)
	elif [ "$var" == "capacity" ] || [ "$var" == "affinity" ]; then
		set_${var} ${!var}
	elif [ "$var" == "plugins" ]; then
		foreach source ${plugins//:/ } >/dev/null
	fi

	return 0
}

unset_config() {
	local var=$1
	local show_val="$var[@]"
	local val_old="${!show_val}"
	eval $var=
	erase_from configs $var

	if [ "$var" == "name" ]; then
		name=$(name)
		log "node name has been changed, register${name:+ $name} on the chat system..."
		echo "name${name:+ $name}"
	elif [ "$var" == "brokers" ] || [ "$var" == "broker" ]; then
		brokers=()
		foreach discard_broker $val_old
		erase_from configs brokers broker
	elif [ "$var" == "workers" ] || [ "$var" == "worker" ]; then
		workers=()
		foreach discard_worker $val_old
		erase_from configs workers worker
	elif [ "$var" == "capacity" ] || [ "$var" == "affinity" ]; then
		set_${var} 0
	fi

	return 0
}

init_configs() {
	local mode=$1
	if [[ $mode == broker ]]; then
		contains configs affinity || affinity=
	elif [[ $mode == worker ]]; then
		contains configs capacity || capacity=
		if ! contains configs affinity; then
			{ affinity=$capacity; } 2>/dev/null
			(( affinity )) || affinity=$(nproc)
			capacity=
		fi
	fi
}

init_register() {
	local name=$1
	registered=$name
	set_capacity ${capacity}
	set_affinity ${affinity:-0}
	observe_overview; observe_status
	log "initialized: state ${system_status[@]}"
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
		discard_worker $name
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
	local item=${1:-null} who=${2?}
	local subscribers=(${notify[$item]})
	erase_from subscribers $who
	subscribers+=($who)
	notify[$item]=${subscribers[@]}
	news[$item-$who]=subscribe
}

unsubscribe() {
	local item=${1:-null} who=${2?}
	local subscribers=(${notify[$item]})
	erase_from subscribers $who
	notify[$item]=${subscribers[@]}
	[[ ${notify[$item]} ]] || unset notify[$item]
	unset news[$item-$who]
}

change_brokers() {
	local current=($1) pending=($2)
	erase_from pending $name
	local added=(${pending[@]}) removed=(${current[@]})
	erase_from added ${current[@]}
	erase_from removed ${pending[@]}
	log "confirm brokers change: (${current[@]}) --> (${pending[@]})"
	foreach discard_broker ${removed[@]}
	foreach contact_broker ${added[@]}
	brokers=(${pending[@]})
}

change_workers() {
	local current=($1) pending=($2)
	if [[ -v state[$name] ]]; then
		erase_from current $name
		current+=($name)
	fi
	local added=(${pending[@]}) removed=(${current[@]})
	erase_from added ${current[@]}
	erase_from removed ${pending[@]}
	log "confirm workers change: (${current[@]}) --> (${pending[@]})"
	foreach discard_worker ${removed[@]}
	foreach contact_worker ${added[@]}
	workers=(${pending[@]})
}

contact_broker() {
	local broker=${1?}
	if [[ $broker != $name ]]; then
		log "contact $broker for handshake (protocol 0)"
		echo "$broker << use protocol 0"
	fi
}

contact_worker() {
	local worker=${1?}
	if [[ $worker != $name ]]; then
		log "contact $worker for worker state"
		echo "$worker << report state"
	else
		[[ -v state[$name] ]] || set_affinity $affinity
		[[ -v state[$name] ]] && log "contact $name, verify that $name state ${state[$name]/:/ }" || \
			log "unable to contact $name"
	fi
}

discard_broker() {
	local broker=${1?}
	log "discard broker: $broker"
	unsubscribe state "$broker"
	if [[ ${discard_at_remote-broker+worker} == *"broker"* && $broker != $name ]]; then
		log "forward discard worker to $broker"
		echo "$broker << operate discard worker $name"
	fi
}

discard_worker() {
	local worker=${1?}
	log "discard worker: $worker"
	local ids=$(filter_keys assign $worker)
	foreach terminate $ids
	unset state[$worker] hold[$worker]
	if [[ ${discard_at_remote-broker+worker} == *"worker"* && $worker != $name ]]; then
		log "forward discard broker to $worker"
		echo "$worker << operate discard broker $name"
	fi
	queue=($ids ${queue[@]})
	[[ $ids ]] && log "revoke assigned request $ids, queue = ($(omit ${queue[@]}))"
}

discard_assets() {
	local id=${1?}
	log "discard assets: $id"
	terminate $id
	unset cmd[$id] own[$id] res[$id] tmout[$id] tmdue[$id] prefer[$id] stdin[$id] stdout[$id]
}

prepare_shutdown() {
	declare -g name=$name
	foreach kill_request ${!pid[@]}
	exit_code=0
}

prepare_restart() {
	foreach kill_request ${!pid[@]}
	[[ $tcp_fd ]] && exec 0<&- 1>&-
	[[ $res_fd ]] && exec {res_fd}<&- {res_fd}>&-
}

node_login() {
	local who=$1
	cpfx log log_src
	log() {
		cpfx log_src log
		log "node $who logged in"
		log "$@"
	}
	contains brokers $who && log "broker $who connected" && contact_broker $who
	contains workers $who && log "worker $who connected" && contact_worker $who
	mvfx log_src log
}

node_logout() {
	local who=$1
	cpfx log log_src
	log() {
		cpfx log_src log
		log "node $who logged out"
		log "$@"
	}
	contains brokers $who && log "broker $who disconnected, wait until $who come back..."
	contains workers $who && log "worker $who disconnected, wait until $who come back..."

	local discard_at_remote="none"
	[[ -v state[$who] ]] && unset state[$who] && discard_worker $who
	[[ -v news[state-$who] ]] && discard_broker $who

	local item
	for item in $(vfmt=" %s " filter_keys notify "* $who *"); do
		log "unsubscribe $item for $who"
		unsubscribe $item $who
	done

	if contains own $who && [[ ! ${keep_orphan_assets} ]]; then
		foreach discard_assets $(filter_keys own $who)
	fi
	mvfx log_src log
}

node_rename() {
	local who=$1 new=$2
	cpfx log log_src
	log() {
		cpfx log_src log
		log "node $who renamed as $new"
		log "$@"
	}

	if contains brokers $who; then
		log "broker $who renamed as $new"
		erase_from brokers $who; brokers+=($new)
	elif contains brokers $new; then
		log "broker $new connected"
		contact_broker $new
	fi
	if contains workers $who; then
		log "worker $who renamed as $new"
		erase_from workers $who; workers+=($new)
	elif contains workers $new; then
		log "worker $new connected"
		contact_worker $new
	fi

	if [[ -v state[$who] ]]; then
		log "transfer worker state: $who -> $new"
		state[$new]=${state[$who]}
		hold[$new]=${hold[$who]}
		unset state[$who] hold[$who]
		local id
		for id in $(filter_keys assign $who); do
			assign[$id]=$new
		done
	fi

	local id
	for id in $(filter_keys own $who); do
		log "transfer ownership $id: $who -> $new"
		own[$id]=$new
	done

	local item
	for item in $(vfmt=" %s " filter_keys notify "* $who *"); do
		log "transfer subscription $item: $who -> $new"
		unsubscribe $item $who
		subscribe $item $new
	done
	mvfx log_src log
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

init_system_io() {
	trap 'log "$(name) has been interrupted"; exit 64' INT
	trap 'log "$(name) has been terminated"; exit 64' TERM
	trap 'code=$?; cleanup; log "$(name) is terminated"; exit $code' EXIT

	local endpoint=$(printf "%s\n" "$addr:$port" "${configs[@]}" | grep -E "^([^:=]+):([0-9]+)$")
	local addr port; IFS=: read -r addr port <<< $endpoint
	if [[ $addr ]] && [[ $port ]]; then
		log "connect to chat system at $addr:$port..."
		while ! { exec {tcp_fd}<>/dev/tcp/$addr/$port; } 2>/dev/null; do
			log "failed to connect $addr:$port, host down?"
			local io_count=${io_count:-0}
			if (( $((++io_count)) >= ${max_io_count:-65536} )); then
				log "max number of connections is reached"
				return $((exit_code=15))
			fi
			log "wait ${wait_for_conn:-60}s before the next attempt..."
			sleep ${wait_for_conn:-60}
		done
		log "connected to chat system successfully"
		if ! { exec 0<&$tcp_fd 1>&$tcp_fd {tcp_fd}>&-; } 2>/dev/null; then
			log "failed to redirect input/output to chat system"
			return $((exit_code=14))
		fi
	fi

	if [[ $buffered_input ]]; then
		if { exec {buffered_input}<&0 0< <(
			stdin_buf=$(mktemp /tmp/$(basename $0 .sh).XXXX.stdin)
			cat <&$buffered_input {buffered_input}<&- >$stdin_buf &
			sleep ${system_tick:-0.1}
			tail -s ${system_tick:-0.1} -f $stdin_buf {buffered_input}<&- &
			trap 'kill $(jobs -p) 2>&-; rm -f $stdin_buf 2>&-' EXIT
			wait -n
		) {buffered_input}<&-; } 2>/dev/null; then
			log "applied buffered input successfully"
		else
			log "failed to apply buffered input at $stdin_buf"
			return $((exit_code=14))
		fi
	fi

	if { exec {res_fd}<> <(:); } 2>/dev/null; then
		log "initialized response pipe successfully"
	else
		log "failed to initialize response pipe"
		return $((exit_code=13))
	fi

	return 0
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

millisec() {
	[[ $1 =~ ^[0-9]* ]]
	case "${1:${#BASH_REMATCH}}" in
		h*) echo $((BASH_REMATCH*3600000)); ;;
		ms) echo $((BASH_REMATCH)); ;;
		m*) echo $((BASH_REMATCH*60000)); ;;
		*)  echo $((BASH_REMATCH*1000)); ;;
	esac
}

foreach() {
	local run=$1 code=0 arg
	for arg in "${@:2}"; do $run "$arg"; code=$((code|$?)); done
	return $code
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

command_not_found_handle() {
	[[ $1 != handle_*_input ]] && echo "$(basename $0): $1: command not found" >&2
	return 127
}

override() {
	local -n level=${1}_override_level
	cpfx ${1} ${1}_override_$((level++))
	[[ ${2} ]] && cpfx ${2} ${1}
}

invoke_overridden() {
	local level=${invoke_level:-${1}_override_level}
	(( level-- )) || return 255
	invoke_level=$level ${1}_override_$level "${@:2}"
}

undo_override() {
	local -n level=${1}_override_level
	(( level )) || return 255
	mvfx ${1}_override_$((--level)) ${1}
}

cpfx() { local fx; fx=$(declare -f ${1?}) && eval "${fx/$1/${2?}}"; }

rmfx() { unset -f $@; }

mvfx() { cpfx $1 $2 && rmfx $1; }

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

name() { echo ${name:-$(basename "$0" .sh)}; }

log() { echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $@" >&2; }

override log
log() {
	logfile=${logfile:-$(name)_$(date '+%Y%m%d_%H%M%S_%3N').log}
	if exec 3>> "$logfile" && flock -xn 3; then
		override cleanup
		cleanup() { invoke_overridden cleanup; flock -u 3 2>/dev/null; }
		exec 2> >(trap '' INT TERM; exec tee /dev/fd/2 >&3)
	fi

	undo_override log
	log "$@"
}

#### script main ####
if [ "$0" == "$BASH_SOURCE" ]; then
	main "$@"
fi
