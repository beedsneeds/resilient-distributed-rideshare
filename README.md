# resilient-distributed-rideshare
A simple distributed rideshare system demonstrating resilience patterns (outbox, dedup,  crash-recovery and reconciler) across services

## Setup

Launch the project in a [devcontainer](https://code.visualstudio.com/docs/devcontainers/containers)


Run each command in a separate terminal: 


```bash
cd services/ride && go run .        # gRPC server, owns ride lifecycle

cd services/matching && go run .    # consumes ride.requested, assigns drivers

cd services/rider && go run .       # gRPC client, generates ride requests

cd services/reconciler && go run .  # background reconciler for orphaned state
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

Optional: Reset state between runs with `./faultinject/resetstate.sh`

Troubleshooting: If the services do not recover, reset db state and try again. If that doesn't work, let me know