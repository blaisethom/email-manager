#!/usr/bin/env bash
# Run email-analyser pipeline one company at a time to control memory usage.
# Each company runs as a foreground subprocess with a timeout.
# Tracks completed companies in .pipeline_progress for resume capability.

set -eo pipefail

COMPANY_FILE="companies.txt"
PROGRESS_FILE=".pipeline_progress"
STAGES="-s extract_events -s discover_discussions -s analyse_discussions -s propose_actions"
ANALYSER=".venv/bin/email-analyser"
TIMEOUT_SEC=900  # 15 min per company max

# Extract domains (first column) from company file, skip comments/blanks
mapfile -t COMPANIES < <(awk 'NF && !/^#/ { print $1 }' "$COMPANY_FILE")
TOTAL=${#COMPANIES[@]}

# Load already-completed companies
touch "$PROGRESS_FILE"
declare -A DONE
while IFS= read -r line; do
    [[ -n "$line" ]] && DONE["$line"]=1
done < "$PROGRESS_FILE"

get_mem_mb() {
    awk '/MemAvailable/ {printf "%.0f", $2/1024}' /proc/meminfo
}

echo "=== Pipeline runner ==="
echo "Total companies: $TOTAL"
echo "Already completed: ${#DONE[@]}"
echo "Timeout per company: ${TIMEOUT_SEC}s"
echo ""

SUCCEEDED=0
FAILED=0
SKIPPED=0

for i in "${!COMPANIES[@]}"; do
    DOMAIN="${COMPANIES[$i]}"
    IDX=$((i + 1))

    if [[ -n "${DONE[$DOMAIN]:-}" ]]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Wait for memory to be reasonable before starting
    while true; do
        AVAIL=$(get_mem_mb)
        if [[ "$AVAIL" -gt 1000 ]]; then
            break
        fi
        echo "  Waiting for memory (available: ${AVAIL}MB, need >1000MB)..."
        sleep 10
    done

    AVAIL=$(get_mem_mb)
    echo "[$IDX/$TOTAL] $DOMAIN  (mem: ${AVAIL}MB)"

    # Run synchronously with timeout
    if timeout "$TIMEOUT_SEC" $ANALYSER analyse $STAGES --company "$DOMAIN" >> "pipeline_${DOMAIN}.log" 2>&1; then
        echo "  OK: $DOMAIN"
        echo "$DOMAIN" >> "$PROGRESS_FILE"
        SUCCEEDED=$((SUCCEEDED + 1))
    else
        EC=$?
        if [[ $EC -eq 124 ]]; then
            echo "  TIMEOUT: $DOMAIN (>${TIMEOUT_SEC}s)"
        else
            echo "  FAIL($EC): $DOMAIN"
        fi
        FAILED=$((FAILED + 1))
    fi

    # Let memory settle
    sleep 3
done

echo ""
echo "=== Summary ==="
echo "Succeeded: $SUCCEEDED"
echo "Failed:    $FAILED"
echo "Skipped:   $SKIPPED (already done)"
echo "Total:     $TOTAL"
