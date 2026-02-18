
## Coverage Measurement

**Always use this exact sequence for reliable coverage numbers:**
```bash
rm -rf _build/test/cover && rebar3 eunit --cover && rebar3 cover --verbose
```

Do NOT run partial test suites or multiple coverage runs - each run overwrites the previous coverdata.
