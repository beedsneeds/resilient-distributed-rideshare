# resilient-distributed-rideshare

A simple distributed rideshare system demonstrating resilience patterns (outbox, dedup, crash-recovery and reconciler) across services

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

## Design

I initially set out to build a rideshare backend to better learn system, but ended up building the substrate underneath a workflow engine ([Temporal](https://temporal.io/blog/workflow-engine-principles)) — transfer queues, task leases, idempotent task application.

https://tb-static.uber.com/prod/udam-assets/55c1c0e0-cbec-5737-a33f-95ae971ff28d.png
Implemented all non-human UX failure modes here except for location.
I chose a subset to model, because if you read the doc, you'll notice much of the patterns are the same (and those are the failure modes I tested)
Demonstrate mechanism without the scope creep
Src: Uber Blog | Ground-up Rearchitecture of Uber's Fulfillment Platform https://www.uber.com/us/en/blog/fulfillment-platform-rearchitecture/
