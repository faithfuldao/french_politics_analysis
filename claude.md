# Project: politics_graph_relations

## Agent team

This project uses a shared multi-agent team defined in `~/.claude/agents/`. You can call any agent directly or let the Team Lead coordinate.

| Agent | Invoke when |
|-------|-------------|
| **Team Lead** | Task spans multiple domains or you're not sure who should handle it |
| **Frontend Dev** | Any UI work — components, pages, forms, layouts using shadcn/ui |
| **Backend Dev** | Neo4j schema, Cypher queries, Python, FastAPI, API routes, server logic, Graph Rag, Agentic AI |
| **Cybersecurity Dev** | Security audit, before shipping auth/input/data features |

## Stack

- Frontend: NextJs, TypeScript, Tailwind CSS, shadcn/ui
- Database: AI agent, Neo4j (Cypher), python, FastAPI, GraphRag structure
- Auth: no auth free desktop app platform

## Conventions

- Always run the Cybersecurity Dev before considering a feature complete if it involves user input, authentication, or data persistence.
- Cypher queries must always use parameterized syntax (`$param`) — never string interpolation.
- UI components must use shadcn primitives before reaching for custom implementations.
