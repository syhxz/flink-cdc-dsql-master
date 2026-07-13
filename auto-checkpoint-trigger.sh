#!/bin/bash

# Auto-Checkpoint Trigger Service for Flink CDC
# This script monitors the Flink CDC pipeline and automatically triggers checkpoints
# when the pipeline is waiting for checkpoint completion to transition to incremental mode

FLINK_REST_URL="http://localhost:8081"
CHECK_INTERVAL=10  # Check every 10 seconds
MAX_WAIT_TIME=300  # Maximum wait time (5 minutes) before triggering checkpoint
LOG_FILE="/tmp/auto-checkpoint-trigger.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

get_running_jobs() {
    curl -s "$FLINK_REST_URL/jobs" 2>/dev/null | jq -r '.jobs[]? | select(.status == "RUNNING") | .id' 2>/dev/null || echo ""
}

check_pipeline_waiting_for_checkpoint() {
    local job_id=$1
    local log_file="/root/flink-cdc-master/flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/target/flink-1.20.1/log/flink-yonghs-standalonesession-0-80a9974765ce.log"
    
    # Check if pipeline is waiting for checkpoint completion
    if [[ -f "$log_file" ]] && grep -q "waiting for a complete checkpoint to mark the assigner finished" "$log_file"; then
        # Check if this message appeared recently (within last 5 minutes)
        local recent_wait=$(tail -200 "$log_file" | grep "waiting for a complete checkpoint" | tail -1)
        if [[ -n "$recent_wait" ]]; then
            # Extract timestamp and check if it's recent
            local timestamp=$(echo "$recent_wait" | grep -o "2025-[0-9-]* [0-9:]*")
            if [[ -n "$timestamp" ]]; then
                local wait_time=$(date -j -f "%Y-%m-%d %H:%M:%S" "$timestamp" "+%s" 2>/dev/null || echo "0")
                local current_time=$(date "+%s")
                local time_diff=$((current_time - wait_time))
                
                # If the wait message is less than 10 minutes old, consider it active
                if [[ $time_diff -lt 600 ]]; then
                    return 0  # Pipeline is waiting for checkpoint
                fi
            fi
        fi
    fi
    return 1  # Pipeline is not waiting
}

trigger_checkpoint() {
    local job_id=$1
    log "🔧 Triggering checkpoint for job: $job_id"
    
    local response=$(curl -s -X POST "$FLINK_REST_URL/jobs/$job_id/checkpoints" 2>/dev/null)
    local request_id=$(echo "$response" | jq -r '.["request-id"]' 2>/dev/null)
    
    if [[ "$request_id" != "null" && -n "$request_id" ]]; then
        log "✅ Checkpoint triggered successfully. Request ID: $request_id"
        return 0
    else
        log "❌ Failed to trigger checkpoint. Response: $response"
        return 1
    fi
}

monitor_checkpoint_completion() {
    local job_id=$1
    local log_file="/root/flink-cdc-master/flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/target/flink-1.20.1/log/flink-yonghs-standalonesession-0-80a9974765ce.log"
    
    log "⏳ Monitoring checkpoint completion for job: $job_id"
    
    # Wait up to 60 seconds for checkpoint completion
    for i in {1..12}; do
        sleep 5
        if [[ -f "$log_file" ]] && grep -q "Snapshot split assigner is turn into finished status" "$log_file"; then
            # Check if this completion is recent (within last 2 minutes)
            local recent_completion=$(tail -50 "$log_file" | grep "Snapshot split assigner is turn into finished status" | tail -1)
            if [[ -n "$recent_completion" ]]; then
                local timestamp=$(echo "$recent_completion" | grep -o "2025-[0-9-]* [0-9:]*")
                if [[ -n "$timestamp" ]]; then
                    local completion_time=$(date -j -f "%Y-%m-%d %H:%M:%S" "$timestamp" "+%s" 2>/dev/null || echo "0")
                    local current_time=$(date "+%s")
                    local time_diff=$((current_time - completion_time))
                    
                    # If completion happened within last 2 minutes, it's likely our checkpoint
                    if [[ $time_diff -lt 120 ]]; then
                        log "🎉 Pipeline successfully transitioned to incremental mode!"
                        return 0
                    fi
                fi
            fi
        fi
    done
    
    log "⚠️  Checkpoint completion not detected within 60 seconds (may still be in progress)"
    return 1
}

main() {
    log "🚀 Starting Auto-Checkpoint Trigger Service v2.0"
    log "📊 Monitoring Flink REST API: $FLINK_REST_URL"
    log "⏱️  Check interval: ${CHECK_INTERVAL}s"
    log "⏰ Max wait time: ${MAX_WAIT_TIME}s"
    
    local wait_start_time=0
    local checkpoint_triggered=false
    local last_job_id=""
    
    while true; do
        local jobs=$(get_running_jobs)
        
        if [[ -z "$jobs" ]]; then
            log "⏳ No running jobs found. Waiting..."
            sleep "$CHECK_INTERVAL"
            continue
        fi
        
        for job_id in $jobs; do
            # Reset state if job ID changed
            if [[ "$job_id" != "$last_job_id" ]]; then
                log "🔄 New job detected: $job_id (previous: $last_job_id)"
                wait_start_time=0
                checkpoint_triggered=false
                last_job_id="$job_id"
            fi
            
            log "🔍 Checking job: $job_id"
            
            if check_pipeline_waiting_for_checkpoint "$job_id"; then
                if [[ $wait_start_time -eq 0 ]]; then
                    wait_start_time=$(date +%s)
                    log "⏳ Pipeline is waiting for checkpoint completion. Starting timer..."
                fi
                
                local current_time=$(date +%s)
                local wait_duration=$((current_time - wait_start_time))
                
                log "📊 Pipeline has been waiting for ${wait_duration}s (max: ${MAX_WAIT_TIME}s)"
                
                if [[ $wait_duration -ge $MAX_WAIT_TIME && "$checkpoint_triggered" == "false" ]]; then
                    log "🚨 Max wait time reached. Auto-triggering checkpoint..."
                    
                    if trigger_checkpoint "$job_id"; then
                        checkpoint_triggered=true
                        
                        if monitor_checkpoint_completion "$job_id"; then
                            log "🎉 Auto-checkpoint trigger successful! Pipeline is now in incremental mode."
                            log "✅ Service completed successfully. Exiting."
                            exit 0
                        else
                            log "⚠️  Checkpoint may still be in progress. Continuing monitoring..."
                        fi
                    else
                        log "❌ Failed to trigger checkpoint. Will retry in next cycle."
                    fi
                fi
            else
                if [[ $wait_start_time -ne 0 ]]; then
                    log "✅ Pipeline is no longer waiting for checkpoint. Resetting timer."
                    wait_start_time=0
                    checkpoint_triggered=false
                fi
            fi
        done
        
        sleep "$CHECK_INTERVAL"
    done
}

# Handle script termination
trap 'log "🛑 Auto-Checkpoint Trigger Service stopped"; exit 0' SIGINT SIGTERM

# Start the service
main "$@"
