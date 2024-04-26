# 15721-s24-scheduler2
15-721 Spring 2024 - Scheduler #2

## TODO
- Priority queue task selection logic
- Support for intra-QUERY parallelism
- Scheduler server logging with [`tracing`]
- Better error handling with [`anyhow`] or [`thiserror`]

- Changes made:
- 1. Refactor frontend, support submitting batch SQL jobs and timing of individual query
- 2. Implement executor execution plan rewrite
- 3. Add recordbatch serialization/deserialization
- 4. 
