# resilient-distributed-rideshare


## Setup

Run each command in a separate terminal: 

go run . 2>&1 | tee ./service.log

```bash
cd services/ride && go run . 2>&1 | tee /tmp/ride-service.log

cd services/matching && go run . 2>&1 | tee /tmp/matching-service.log

cd services/rider && go run . 2>&1 | tee /tmp/rider-service.log

cd services/reconciler && go run . 2>&1 | tee /tmp/rider-service.log
```

After you've inspected the normal behavior of the system, in another terminal, run the interactive script and follow the prompts

```bash
./faultinject/faults.sh <failure-scenario>
``` 

Supported Failure Scenarios: 
- matching-ghost-message
- ride-ghost-message
- matching-claim-timeout
- ride-claim-timeout
- matching-pel-recovery
- ride-accepted-rollback
- ride-request-retry

