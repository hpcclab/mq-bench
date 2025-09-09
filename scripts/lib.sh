#!/usr/bin/env bash
set -euo pipefail

# Helper: default VAR to value if unset/empty
def() { local var="$1" val="$2"; eval "[ -n \"\${$var-}\" ] || $var=\"$val\""; }

# Helper: print a header
hdr() { echo "==== $* ===="; }

# Helper: build release binary if missing
build_release_if_needed() {
	local bin="${1:-./target/release/mq-bench}"
	if [[ ! -x "${bin}" ]]; then
		echo "[build] Building release binary..."
		cargo build --release
	fi
}

# Helper: make connect args based on ENGINE and role (sub|pub)
# Usage: make_connect_args sub OUT_ARR   or   make_connect_args pub OUT_ARR
make_connect_args() {
	local role="${1:?role sub|pub}"; shift
	local -n _out="${1:?out array var name}"; shift || true
	local engine="${ENGINE:-zenoh}"
	_out=()
	case "${engine}" in
		zenoh)
			# Accept both ENDPOINT_* and legacy *_ENDPOINT variable names
			local ep
			if [[ "${role}" == "sub" ]]; then
				ep="${ENDPOINT_SUB:-${SUB_ENDPOINT:-tcp/127.0.0.1:7447}}"
			else
				ep="${ENDPOINT_PUB:-${PUB_ENDPOINT:-tcp/127.0.0.1:7447}}"
			fi
			if [[ -n "${ZENOH_MODE:-}" ]]; then
				_out=(--engine zenoh --connect "endpoint=${ep}" --connect "mode=${ZENOH_MODE}")
			else
				_out=(--engine zenoh --endpoint "${ep}")
			fi
			;;
		zenoh-peer)
			# Accept both ENDPOINT_* and legacy *_ENDPOINT variable names
			local ep
			if [[ "${role}" == "sub" ]]; then
				ep="${ENDPOINT_SUB:-${SUB_ENDPOINT:-tcp/127.0.0.1:7447}}"
			else
				ep="${ENDPOINT_PUB:-${PUB_ENDPOINT:-tcp/127.0.0.1:7447}}"
			fi
			_out=(--engine zenoh --connect "endpoint=${ep}" --connect "mode=peer")
			;;
		mqtt)
			# Generic MQTT engine. Requires MQTT_HOST and MQTT_PORT. Optional MQTT_USERNAME/MQTT_PASSWORD.
			local host="${MQTT_HOST:-127.0.0.1}"; local port="${MQTT_PORT:-1883}"
			local user_opt=() pass_opt=()
			if [[ -n "${MQTT_USERNAME:-}" ]]; then user_opt=(--connect "username=${MQTT_USERNAME}"); fi
			if [[ -n "${MQTT_PASSWORD:-}" ]]; then pass_opt=(--connect "password=${MQTT_PASSWORD}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		amqp)
			# Deprecated: use ENGINE=rabbitmq. Keep as alias.
			local host="${RABBITMQ_HOST:-127.0.0.1}"
			local port="${RABBITMQ_PORT:-5672}"
			local user="${RABBITMQ_USER:-guest}"
			local pass="${RABBITMQ_PASS:-guest}"
			local vhost="${RABBITMQ_VHOST:-/}"
			if [[ "$vhost" == "/" ]]; then vhost="%2f"; fi
			local url="amqp://${user}:${pass}@${host}:${port}/${vhost}"
			_out=(--engine rabbitmq --connect "url=${url}")
			;;
		rabbitmq)
			# RabbitMQ over AMQP (native adapter); default
			local host="${RABBITMQ_HOST:-127.0.0.1}"
			local port="${RABBITMQ_PORT:-5672}"
			local user="${RABBITMQ_USER:-guest}"
			local pass="${RABBITMQ_PASS:-guest}"
			local vhost="${RABBITMQ_VHOST:-/}"
			if [[ "$vhost" == "/" ]]; then vhost="%2f"; fi
			local url="amqp://${user}:${pass}@${host}:${port}/${vhost}"
			_out=(--engine rabbitmq --connect "url=${url}")
			;;
		rabbitmq-amqp)
			# Explicit RabbitMQ over AMQP
			local host="${RABBITMQ_HOST:-127.0.0.1}"
			local port="${RABBITMQ_PORT:-5672}"
			local user="${RABBITMQ_USER:-guest}"
			local pass="${RABBITMQ_PASS:-guest}"
			local vhost="${RABBITMQ_VHOST:-/}"
			if [[ "$vhost" == "/" ]]; then vhost="%2f"; fi
			local url="amqp://${user}:${pass}@${host}:${port}/${vhost}"
			_out=(--engine rabbitmq --connect "url=${url}")
			;;
		rabbitmq-mqtt)
			# RabbitMQ via MQTT plugin
			local host="${RABBITMQ_HOST:-127.0.0.1}"
			local port="${RABBITMQ_MQTT_PORT:-1886}"
			local user_opt=() pass_opt=()
			if [[ -n "${MQTT_USERNAME:-}" ]]; then user_opt=(--connect "username=${MQTT_USERNAME}"); fi
			if [[ -n "${MQTT_PASSWORD:-}" ]]; then pass_opt=(--connect "password=${MQTT_PASSWORD}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		mqtt-mosquitto)
			local host="${MOSQUITTO_HOST:-127.0.0.1}"
			local port="${MOSQUITTO_PORT:-1883}"
			local user_opt=() pass_opt=()
			if [[ -n "${MQTT_USERNAME:-}" ]]; then user_opt=(--connect "username=${MQTT_USERNAME}"); fi
			if [[ -n "${MQTT_PASSWORD:-}" ]]; then pass_opt=(--connect "password=${MQTT_PASSWORD}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		mqtt-emqx)
			local host="${EMQX_HOST:-127.0.0.1}"
			local port="${EMQX_PORT:-1884}"
			local user_opt=() pass_opt=()
			if [[ -n "${MQTT_USERNAME:-}" ]]; then user_opt=(--connect "username=${MQTT_USERNAME}"); fi
			if [[ -n "${MQTT_PASSWORD:-}" ]]; then pass_opt=(--connect "password=${MQTT_PASSWORD}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		mqtt-hivemq)
			local host="${HIVEMQ_HOST:-127.0.0.1}"
			local port="${HIVEMQ_PORT:-1885}"
			local user_opt=() pass_opt=()
			if [[ -n "${MQTT_USERNAME:-}" ]]; then user_opt=(--connect "username=${MQTT_USERNAME}"); fi
			if [[ -n "${MQTT_PASSWORD:-}" ]]; then pass_opt=(--connect "password=${MQTT_PASSWORD}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		mqtt-rabbitmq)
			local host="${RABBITMQ_HOST:-127.0.0.1}"
			local port="${RABBITMQ_MQTT_PORT:-1886}"
			local user_opt=() pass_opt=()
			if [[ -n "${MQTT_USERNAME:-}" ]]; then user_opt=(--connect "username=${MQTT_USERNAME}"); fi
			if [[ -n "${MQTT_PASSWORD:-}" ]]; then pass_opt=(--connect "password=${MQTT_PASSWORD}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		mqtt-artemis)
			local host="${ARTEMIS_HOST:-127.0.0.1}"
			local port="${ARTEMIS_MQTT_PORT:-1887}"
			# Defaults from docker-compose.yml: ARTEMIS_USERNAME=admin, ARTEMIS_PASSWORD=admin
			# Allow overrides via MQTT_USERNAME/MQTT_PASSWORD or ARTEMIS_MQTT_USERNAME/ARTEMIS_MQTT_PASSWORD
			local username="${MQTT_USERNAME:-${ARTEMIS_MQTT_USERNAME:-${ARTEMIS_USERNAME:-admin}}}"
			local password="${MQTT_PASSWORD:-${ARTEMIS_MQTT_PASSWORD:-${ARTEMIS_PASSWORD:-admin}}}"
			local user_opt=() pass_opt=()
			if [[ -n "${username}" ]]; then user_opt=(--connect "username=${username}"); fi
			if [[ -n "${password}" ]]; then pass_opt=(--connect "password=${password}"); fi
			_out=(--engine mqtt --connect "host=${host}" --connect "port=${port}" "${user_opt[@]}" "${pass_opt[@]}")
			;;
		nats)
			local host="${NATS_HOST:-127.0.0.1}"
			local port="${NATS_PORT:-4222}"
			_out=(--engine nats --connect "host=${host}" --connect "port=${port}")
			;;
		redis)
			local url="${REDIS_URL:-redis://127.0.0.1:6379}"
			_out=(--engine redis --connect "url=${url}")
			;;
		*)
			echo "[lib] Unsupported ENGINE='${engine}'" >&2
			return 1
			;;
	esac
}

# Start subscriber(s). Sets PID into out var name.
# Args: OUT_PID_VAR  expr  subscribers  csv_path  log_path
start_sub() {
	local -n _outpid="${1:?out pid var}"; shift
	local expr="${1:?expr}"; shift
	local subs="${1:?subscribers}"; shift
	local csv="${1:?csv}"; shift
	local log="${1:?log}"; shift
	local BIN="${BIN:-./target/release/mq-bench}"
	local SNAPSHOT="${SNAPSHOT:-5}"
	local args=()
	make_connect_args sub args
	echo "[sub] ${ENGINE:-zenoh} → ${expr} (subs=${subs})"
	"${BIN}" --snapshot-interval "${SNAPSHOT}" sub \
		"${args[@]}" \
		--expr "${expr}" \
		--subscribers "${subs}" \
		--csv "${csv}" \
		>"${log}" 2>&1 &
	_outpid=$!
}

# Start publisher. Sets PID into out var name.
# Args: OUT_PID_VAR  topic_prefix  payload  rate  duration  csv_path  log_path
start_pub() {
	local -n _outpid="${1:?out pid var}"; shift
	local topic="${1:?topic}"; shift
	local payload="${1:?payload}"; shift
	local rate="${1:-}"; shift || true
	local duration="${1:?duration}"; shift
	local csv="${1:?csv}"; shift
	local log="${1:?log}"; shift
	local BIN="${BIN:-./target/release/mq-bench}"
	local SNAPSHOT="${SNAPSHOT:-5}"
	local args=()
	make_connect_args pub args
	local rate_flag=()
	if [[ -n "${rate}" ]] && (( rate > 0 )); then rate_flag=(--rate "${rate}"); fi
	echo "[pub] ${ENGINE:-zenoh} → ${topic} (payload=${payload}, rate=${rate:-max}, dur=${duration}s)"
	"${BIN}" --snapshot-interval "${SNAPSHOT}" pub \
		"${args[@]}" \
		--topic-prefix "${topic}" \
		--payload "${payload}" \
		"${rate_flag[@]}" \
		--duration "${duration}" \
		--csv "${csv}" \
		>"${log}" 2>&1 &
	_outpid=$!
}

# Print periodic status using last rows
print_status_common() {
	local sub_file="$1" pub_file="$2"
	local last_sub last_pub
	last_sub=$(tail -n +2 "$sub_file" 2>/dev/null | tail -n1 || true)
	last_pub=$(tail -n +2 "$pub_file" 2>/dev/null | tail -n1 || true)
	local spub itpub ttpub rsub itsub p99sub
	if [[ -n "$last_pub" ]]; then
		IFS=, read -r _ spub _ epub ttpub itpub _ _ _ _ _ _ <<<"$last_pub"
	fi
	if [[ -n "$last_sub" ]]; then
		IFS=, read -r _ _ rsub _ _ itsub _ _ p99sub _ _ _ <<<"$last_sub"
	fi
	printf "[status] PUB sent=%s itps=%s tps=%s | SUB recv=%s itps=%s p99=%.2fms\n" \
		"${spub:--}" "${itpub:--}" "${ttpub:--}" \
		"${rsub:--}" "${itsub:--}" "$(awk -v n="${p99sub:-0}" 'BEGIN{printf (n/1e6)}')"
}

# Watch loop until pub exits
watch_until_pub_exits() {
	local pub_pid="$1" sub_csv="$2" pub_csv="$3" snapshot="${SNAPSHOT:-5}"
	echo "[watch] printing status every ${snapshot}s..."
	while kill -0 ${pub_pid} 2>/dev/null; do
		print_status_common "${sub_csv}" "${pub_csv}"
		sleep "${snapshot}"
	done
}

# Final summary printer
summarize_common() {
	local sub_csv="$1" pub_csv="$2"
	echo "=== Summary ==="
	local final_pub final_sub
	final_pub=$(tail -n +2 "${pub_csv}" 2>/dev/null | tail -n1 || true)
	final_sub=$(tail -n +2 "${sub_csv}" 2>/dev/null | tail -n1 || true)
	if [[ -n "$final_pub" ]]; then
		IFS=, read -r _ tsent _ terr tt _ _ _ _ _ _ _ <<<"$final_pub"
		echo "Publisher: sent=${tsent} total_tps=${tt}"
	fi
	if [[ -n "$final_sub" ]]; then
		IFS=, read -r _ _ rcv _ tps _ p50 p95 p99 _ _ _ <<<"$final_sub"
		printf "Subscriber: recv=%s total_tps=%.2f p50=%.2fms p95=%.2fms p99=%.2fms\n" \
			"$rcv" "$tps" "$(awk -v n="$p50" 'BEGIN{printf (n/1e6)}')" \
			"$(awk -v n="$p95" 'BEGIN{printf (n/1e6)}')" "$(awk -v n="$p99" 'BEGIN{printf (n/1e6)}')"
	fi
}

