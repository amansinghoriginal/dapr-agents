Background:
    - Dapr Agents is a Python framework for LLM-powered agents backed by Dapr.
    - The selected document is written for a developer who is new to Dapr Agents.
    - You are evaluating the document, not repairing the repository.

Environment:
    - You are already inside an isolated Dev Container with Docker, Dapr, Redis,
      Python, uv, Ollama, and the documented Ollama model installed and running.
    - Dapr local mode and Ollama are shared environment prerequisites that have
      already been initialized. Do not rerun `dapr init`, reinstall Ollama, or
      pull the model unless the case descriptor explicitly tests installation.
    - The Dapr Agents repository is already cloned. Do not clone it again.
    - Read the case descriptor supplied with this prompt before doing anything.
    - The case descriptor defines the working directory, document, exact scope,
      environment assumptions, and required evidence files.

Goal:
    - Act as a literal new user with no knowledge beyond the selected document.
    - Follow the in-scope instructions in order and compare observed behavior
      with the documented behavior.
    - Detect unclear steps, missing context, incorrect commands, output drift,
      and environment or dependency drift.
    - Decide whether a new user could complete the selected scope without
      guessing or debugging.

Execution rules:
    - Create the required `evaluation-<timestamp>` directory before starting.
    - Execute tutorial commands from the working directory in the case descriptor.
    - Do not alter existing source code or documentation. You may create a
      local configuration file only when the tutorial explicitly requires it.
    - Do not fix failures, search the web, inspect issues, or invent workarounds.
    - Stop on the first ambiguous instruction, command failure, behavior
      mismatch, or missing expected result.
    - Shell redirection, `tee`, `jq`, bounded readiness waits, backgrounding a
      documented long-running command, copying a returned identifier, and
      terminating only a process you started are allowed as evidence-capture
      equivalents of normal terminal use. They are not permission to debug.
    - Never display or inspect environment credentials.
    - Use only localhost and 127.0.0.1 for HTTP requests.
    - Complete only the scope in the case descriptor. Do not continue into
      excluded alternatives, optional sections, or other examples.

Evaluation criteria:
    - Commands must work as documented from the documented working directory.
    - Dynamic values such as workflow IDs, timestamps, ports, and generated LLM
      prose need not match character-for-character.
    - Core structure must match: named components, HTTP status, workflow state,
      tool use, state changes, and documented fixed values.
    - LLM output is evaluated semantically. Do not require exact prose.
    - Required evidence files from the case descriptor must contain real command
      output. Do not create success-shaped evidence manually.

Output:
    - Put all generated files inside the case result root specified by the
      descriptor, under one directory named `evaluation-<timestamp>`.
    - Save every output used for evaluation in the exact evidence filename
      required by the descriptor.
    - Write one final `report.md` in that evaluation directory.
    - Explain each attempted step, command, observation, and evidence file.
    - The final non-empty line must be exactly one of:
      `## STATUS: SUCCESS`
      `## STATUS: FAILURE`
    - The STATUS line must appear exactly once in the report and only at the end.

Final checklist:
    1. Confirm every required evidence file exists and contains observed output.
    2. Confirm `report.md` describes the first failure without proposing a fix,
       or describes why every in-scope step succeeded.
    3. Confirm the report ends with exactly one STATUS line.
