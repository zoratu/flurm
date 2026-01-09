# FLURM Development Resume Guide

## Quick Resume
1. Read this file
2. Read .claude/checkpoint.json
3. Run: `rebar3 compile && rebar3 eunit`
4. Check current phase and next_actions in checkpoint
5. Continue from next_actions list

## Current State
[Auto-updated by checkpoint]

## Parallel Work Available
Check checkpoint.json phases for tasks with status "pending" that don't have dependencies.

## Recent Decisions
Check checkpoint.json decisions array for context on architectural choices.

## If Tests Are Failing
1. Check blockers in checkpoint.json
2. Read the failing test file
3. Read the implementation file
4. Fix and re-run tests
