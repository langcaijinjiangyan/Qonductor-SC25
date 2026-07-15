"""
DAG Engine for Qonductor Workflow Manager.

Parses hybrid Python code (classical + quantum), identifies quantum vs.
classical code sections, tracks I/O dependencies, and builds a Directed
Acyclic Graph (DAG) representation of the workflow.

Based on Qonductor paper Section 5:
"The workflow manager automatically splits a Python file into quantum and
classical code files while maintaining library dependencies and keeping
track of input/output data between the files. Then, the manager creates a
directed acyclic graph (DAG) G = (V, E) where V is the set of classical
and quantum steps and E = {(E_i, E_j) in V x V} are the control and data
flow dependencies between them."
"""

from __future__ import annotations

import ast
import enum
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import networkx as nx


# ---------------------------------------------------------------------------
# DAG data structures
# ---------------------------------------------------------------------------

class StepType(enum.Enum):
    """Type of a workflow step."""
    CLASSICAL = "classical"
    QUANTUM = "quantum"


@dataclass
class DAGNode:
    """A node in the hybrid workflow DAG.

    Attributes:
        step_id: Unique identifier for this step.
        step_type: Whether this is a CLASSICAL or QUANTUM step.
        label: Human-readable label (e.g. function name).
        code: Source code for this step.
        inputs: Variable names consumed by this step.
        outputs: Variable names produced by this step.
        resource_requirements: Optional dict of required resources
            (e.g. {'gpu': 1, 'qpu': 1, 'qubits': 20}).
        metadata: Arbitrary extra information.
    """
    step_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    step_type: StepType = StepType.CLASSICAL
    label: str = ""
    code: str = ""
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    resource_requirements: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.step_id)


@dataclass
class DAGEdge:
    """An edge in the hybrid workflow DAG.

    Represents a data or control dependency between two steps.

    Attributes:
        from_step: Source node ID.
        to_step: Destination node ID.
        data_variable: The variable that creates this dependency.
    """
    from_step: str
    to_step: str
    data_variable: str = ""


# ---------------------------------------------------------------------------
# Hybrid DAG
# ---------------------------------------------------------------------------

class HybridDAG:
    """Directed Acyclic Graph for a hybrid quantum-classical workflow.

    Wraps a networkx.DiGraph and provides convenience methods for
    topological sorting, node/edge querying, and serialization.
    """

    def __init__(self, name: str = "") -> None:
        self.name = name
        self._graph = nx.DiGraph()

    # -- builders -----------------------------------------------------------

    def add_node(self, node: DAGNode) -> None:
        self._graph.add_node(node.step_id, data=node)

    def add_edge(self, edge: DAGEdge) -> None:
        self._graph.add_edge(edge.from_step, edge.to_step, data=edge)

    # -- queries ------------------------------------------------------------

    @property
    def nodes(self) -> list[DAGNode]:
        return [data["data"] for _, data in self._graph.nodes(data=True)]

    @property
    def edges(self) -> list[DAGEdge]:
        return [data["data"] for _, _, data in self._graph.edges(data=True)]

    def get_node(self, step_id: str) -> Optional[DAGNode]:
        try:
            return self._graph.nodes[step_id]["data"]
        except KeyError:
            return None

    @property
    def quantum_nodes(self) -> list[DAGNode]:
        return [n for n in self.nodes if n.step_type == StepType.QUANTUM]

    @property
    def classical_nodes(self) -> list[DAGNode]:
        return [n for n in self.nodes if n.step_type == StepType.CLASSICAL]

    def topological_order(self) -> list[DAGNode]:
        """Return nodes in topological order."""
        order = nx.topological_sort(self._graph)
        return [self._graph.nodes[nid]["data"] for nid in order]

    def predecessors(self, step_id: str) -> list[DAGNode]:
        preds = self._graph.predecessors(step_id)
        return [self._graph.nodes[n]["data"] for n in preds]

    def successors(self, step_id: str) -> list[DAGNode]:
        succs = self._graph.successors(step_id)
        return [self._graph.nodes[n]["data"] for n in succs]

    # -- serialization ------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize the DAG to a JSON-compatible dict."""
        return {
            "name": self.name,
            "nodes": [
                {
                    "step_id": n.step_id,
                    "step_type": n.step_type.value,
                    "label": n.label,
                    "code": n.code,
                    "inputs": n.inputs,
                    "outputs": n.outputs,
                    "resource_requirements": n.resource_requirements,
                    "metadata": n.metadata,
                }
                for n in self.nodes
            ],
            "edges": [
                {
                    "from_step": e.from_step,
                    "to_step": e.to_step,
                    "data_variable": e.data_variable,
                }
                for e in self.edges
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HybridDAG":
        """Deserialize a DAG from a dict."""
        dag = cls(name=data.get("name", ""))
        for nd in data.get("nodes", []):
            dag.add_node(DAGNode(
                step_id=nd["step_id"],
                step_type=StepType(nd["step_type"]),
                label=nd.get("label", ""),
                code=nd.get("code", ""),
                inputs=nd.get("inputs", []),
                outputs=nd.get("outputs", []),
                resource_requirements=nd.get("resource_requirements", {}),
                metadata=nd.get("metadata", {}),
            ))
        for ed in data.get("edges", []):
            dag.add_edge(DAGEdge(
                from_step=ed["from_step"],
                to_step=ed["to_step"],
                data_variable=ed.get("data_variable", ""),
            ))
        return dag

    def __repr__(self) -> str:
        return (f"HybridDAG(name={self.name!r}, "
                f"nodes={len(self.nodes)}, edges={len(self.edges)})")


# ---------------------------------------------------------------------------
# AST-based code analysis
# ---------------------------------------------------------------------------

# Quantum-related modules we detect to classify code sections.
_QUANTUM_IMPORT_PATTERNS: set[str] = {
    "qiskit",
    "qiskit.circuit",
    "qiskit.quantum_info",
    "qiskit.primitives",
    "qiskit_aer",
    "qiskit_ibm_runtime",
    "qiskit_ibm_provider",
    "qiskit.providers",
    "qiskit.algorithms",
    "qiskit.transpiler",
    "qiskit.execute",
    # Qonductor quantum library (Listing 2 style imports).
    "qonductor.lib.quantum",
}

_QUANTUM_CALL_PATTERNS: set[str] = {
    "Sampler",
    "Estimator",
    "Session",
    "transpile",
    "execute",
    "run",
    "backend",
    "QuantumCircuit",
    "QuantumRegister",
    "ClassicalRegister",
}

_QUANTUM_METHOD_CALLS: set[str] = {
    "run",
    "transpile",
    "execute",
    "measure",
    "measure_all",
    "compose",
    "decompose",
}


class _HybridASTVisitor(ast.NodeVisitor):
    """AST visitor that classifies code blocks as classical or quantum.

    Walks the AST and records, for each top-level statement, whether it
    contains references to quantum libraries or constructs.
    """

    def __init__(self) -> None:
        super().__init__()
        self.quantum_lines: set[int] = set()
        self._current_quantum: bool = False
        # Track imports so we can classify later statements.
        self._quantum_aliases: set[str] = set()
        # Track variable definitions and their quantum status.
        self._var_is_quantum: dict[str, bool] = {}

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if any(alias.name.startswith(p) for p in _QUANTUM_IMPORT_PATTERNS):
                self._quantum_aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if any(module.startswith(p) for p in _QUANTUM_IMPORT_PATTERNS):
            for alias in node.names:
                self._quantum_aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # Check if the call is to a quantum function or method.
        if self._is_quantum_call(node):
            self._mark_statement(node)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.generic_visit(node)
        # After visiting, check if value involves quantum calls.
        if hasattr(node, 'value') and self._is_quantum_value(node.value):
            self._mark_statement(node)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self._var_is_quantum[target.id] = True

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.generic_visit(node)
        if self._is_quantum_value(node.value):
            self._mark_statement(node)

    def _is_quantum_call(self, node: ast.Call) -> bool:
        """Check whether a Call node invokes quantum functionality."""
        func = node.func
        # Simple name: e.g. Sampler(), QuantumCircuit()
        if isinstance(func, ast.Name):
            if func.id in _QUANTUM_CALL_PATTERNS:
                return True
            if self._quantum_aliases and func.id in self._quantum_aliases:
                return True
            return self._var_is_quantum.get(func.id, False)
        # Attribute: e.g. qiskit.execute(), backend.run(), circuit.measure_all()
        if isinstance(func, ast.Attribute):
            if func.attr in _QUANTUM_METHOD_CALLS:
                return True
            if isinstance(func.value, ast.Name):
                name = func.value.id
                if name in self._quantum_aliases:
                    return True
                if self._var_is_quantum.get(name, False):
                    return True
        return False

    def _is_quantum_value(self, node: ast.expr) -> bool:
        """Check if an expression involves quantum constructs."""
        if isinstance(node, ast.Call):
            return self._is_quantum_call(node)
        if isinstance(node, ast.Name):
            return self._var_is_quantum.get(node.id, False)
        return False

    def _mark_statement(self, node: ast.AST) -> None:
        """Mark the line(s) of *node* as quantum."""
        if hasattr(node, 'lineno') and node.lineno is not None:
            self.quantum_lines.add(node.lineno)
        if hasattr(node, 'end_lineno') and node.end_lineno is not None:
            for line in range(node.lineno or 0, node.end_lineno + 1):
                self.quantum_lines.add(line)


# ---------------------------------------------------------------------------
# Code Splitter
# ---------------------------------------------------------------------------

class CodeSplitter:
    """Splits hybrid Python source code into quantum and classical sections.

    Uses AST analysis to classify each line as quantum or classical, then
    groups consecutive same-type lines into DAG nodes and infers data
    dependencies between them.
    """

    # Minimum number of lines for a standalone code block.
    MIN_BLOCK_LINES: int = 2

    def __init__(self) -> None:
        self._visitor = _HybridASTVisitor()

    def parse(self, hybrid_code: str, name: str = "") -> HybridDAG:
        """Parse *hybrid_code* and produce a ``HybridDAG``.

        Args:
            hybrid_code: Complete Python source of the hybrid application.
            name: Optional name for the DAG / workflow.

        Returns:
            A ``HybridDAG`` with nodes classified and edges representing
            data dependencies.
        """
        # 1. AST analysis to mark quantum lines.
        tree = ast.parse(hybrid_code)
        self._visitor = _HybridASTVisitor()
        self._visitor.visit(tree)

        # 2. Line-level classification.
        lines = hybrid_code.splitlines(keepends=True)
        line_classifications = self._classify_lines(lines)

        # 3. Group consecutive same-type lines into blocks.
        blocks = self._build_blocks(line_classifications, lines)

        # 4. Build DAG.
        return self._build_dag(blocks, name)

    def _classify_lines(
        self, lines: list[str]
    ) -> list[tuple[int, StepType]]:
        """Classify each line number as CLASSICAL or QUANTUM."""
        result: list[tuple[int, StepType]] = []
        for i, line in enumerate(lines):
            lineno = i + 1
            stripped = line.strip()
            # Blank / comment lines inherit from surrounding context.
            if not stripped or stripped.startswith('#'):
                result.append((lineno, StepType.CLASSICAL))
                continue
            if lineno in self._visitor.quantum_lines:
                result.append((lineno, StepType.QUANTUM))
            else:
                result.append((lineno, StepType.CLASSICAL))
        return result

    @staticmethod
    def _build_blocks(
        classifications: list[tuple[int, StepType]],
        lines: list[str],
    ) -> list[dict]:
        """Group consecutive same-type lines into code blocks.

        Returns a list of dicts with keys: step_type, start_line, end_line, code.
        """
        if not classifications:
            return []

        blocks: list[dict] = []
        current_type = classifications[0][1]
        start = classifications[0][0]
        current_code_lines: list[str] = []

        for lineno, step_type in classifications:
            # Blank / comment lines are merged into the current block type.
            stripped = lines[lineno - 1].strip() if lineno - 1 < len(lines) else ""
            if not stripped or stripped.startswith('#'):
                current_code_lines.append(lines[lineno - 1])
                continue
            if step_type == current_type:
                current_code_lines.append(lines[lineno - 1])
            else:
                blocks.append({
                    "step_type": current_type,
                    "start_line": start,
                    "end_line": lineno - 1,
                    "code": "".join(current_code_lines),
                })
                current_type = step_type
                start = lineno
                current_code_lines = [lines[lineno - 1]]

        # Final block.
        blocks.append({
            "step_type": current_type,
            "start_line": start,
            "end_line": len(lines),
            "code": "".join(current_code_lines),
        })

        # Merge tiny blocks with neighbors to avoid fragmentation.
        return CodeSplitter._merge_small_blocks(blocks)

    @staticmethod
    def _merge_small_blocks(blocks: list[dict]) -> list[dict]:
        """Merge blocks shorter than MIN_BLOCK_LINES with surrounding blocks.

        A small quantum block embedded in classical blocks becomes part of
        the nearest classical block. A small classical block embedded in
        quantum blocks becomes quantum.
        """
        if len(blocks) <= 1:
            return blocks

        merged: list[dict] = []
        i = 0
        while i < len(blocks):
            block = blocks[i]
            code_lines = [l for l in block["code"].splitlines(True)
                          if l.strip() and not l.strip().startswith('#')]
            if len(code_lines) >= CodeSplitter.MIN_BLOCK_LINES:
                merged.append(block)
                i += 1
                continue
            # Tiny block – merge with previous if it exists.
            if merged:
                prev = merged[-1]
                prev["code"] += block["code"]
                prev["end_line"] = block["end_line"]
            else:
                merged.append(block)
            i += 1

        return merged

    @staticmethod
    def _extract_io(block: dict, all_lines: list[str]) -> tuple[list[str], list[str]]:
        """Extract input and output variable names from a code block.

        Uses simple regex heuristics to identify variable assignments
        (outputs) and variable references (inputs) within *block*.
        """
        import re
        code = block["code"]
        # Assignment patterns: var = ...
        assign_pattern = re.compile(r'^\s*(\w+(?:\s*,\s*\w+)*)\s*=\s*', re.MULTILINE)
        outputs = []
        for m in assign_pattern.finditer(code):
            for var in re.split(r'\s*,\s*', m.group(1)):
                v = var.strip()
                if v and not v.startswith('_'):
                    outputs.append(v)

        # Variable reference patterns (simplified heuristic).
        ref_pattern = re.compile(r'\b([a-zA-Z_]\w*)\b')
        all_refs = set(ref_pattern.findall(code))
        # Exclude Python keywords.
        keywords = {
            'import', 'from', 'def', 'class', 'return', 'if', 'else', 'elif',
            'for', 'while', 'try', 'except', 'with', 'as', 'in', 'not', 'and',
            'or', 'is', 'None', 'True', 'False', 'pass', 'break', 'continue',
            'range', 'len', 'print', 'list', 'dict', 'set', 'tuple', 'int',
            'float', 'str', 'bool', 'enumerate', 'zip', 'map', 'filter',
            'partial', 'open', 'yaml', 'self', 'np',
        }
        inputs = list(all_refs - set(outputs) - keywords)
        return inputs, outputs

    def _build_dag(self, blocks: list[dict], name: str) -> HybridDAG:
        """Build a ``HybridDAG`` from classified code blocks."""
        dag = HybridDAG(name=name)
        prev_node: Optional[DAGNode] = None

        for i, block in enumerate(blocks):
            inputs, outputs = self._extract_io(block, [])
            label = f"{block['step_type'].value}_{i + 1}"
            node = DAGNode(
                step_id=f"step_{i + 1}",
                step_type=block["step_type"],
                label=label,
                code=block["code"],
                inputs=inputs,
                outputs=outputs,
                metadata={
                    "start_line": block["start_line"],
                    "end_line": block["end_line"],
                    "index": i,
                },
            )

            # Extract resource requirements from code annotations.
            node.resource_requirements = self._extract_resource_reqs(block["code"])

            dag.add_node(node)

            # Connect to previous node – sequential dependency by default.
            if prev_node is not None:
                dag.add_edge(DAGEdge(
                    from_step=prev_node.step_id,
                    to_step=node.step_id,
                    data_variable="__sequential__",
                ))

            prev_node = node

        # Refine edges: add explicit data dependencies when a node's input
        # matches another node's output.
        for consumer in dag.nodes:
            for producer in dag.nodes:
                if producer.step_id == consumer.step_id:
                    continue
                shared = set(consumer.inputs) & set(producer.outputs)
                for var in shared:
                    # Avoid duplicate edges.
                    existing = {
                        (e.from_step, e.to_step)
                        for e in dag.edges
                    }
                    if (producer.step_id, consumer.step_id) not in existing:
                        dag.add_edge(DAGEdge(
                            from_step=producer.step_id,
                            to_step=consumer.step_id,
                            data_variable=var,
                        ))

        return dag

    @staticmethod
    def _extract_resource_reqs(code: str) -> dict[str, Any]:
        """Extract resource requirement annotations from code comments.

        Recognizes comments like:
            # @resource gpu: 1
            # @resource qubits: 20
        """
        import re
        reqs: dict[str, Any] = {}
        for line in code.splitlines():
            m = re.search(r'#\s*@resource\s+(\w+)\s*:\s*(\S+)', line)
            if m:
                key = m.group(1)
                val = m.group(2)
                try:
                    reqs[key] = int(val)
                except ValueError:
                    try:
                        reqs[key] = float(val)
                    except ValueError:
                        reqs[key] = val
        return reqs

    def split_files(self, dag: HybridDAG) -> tuple[str, str]:
        """Generate separate classical and quantum Python source files.

        Args:
            dag: The DAG to split.

        Returns:
            A ``(classical_code, quantum_code)`` tuple of source strings.
        """
        imports = self._collect_imports(dag)
        classical_body = self._build_code_for_type(dag, StepType.CLASSICAL)
        quantum_body = self._build_code_for_type(dag, StepType.QUANTUM)

        classical = f"{imports}\n\n# === Classical Code ===\n{classical_body}\n"
        quantum = f"{imports}\n\n# === Quantum Code ===\n{quantum_body}\n"
        return classical, quantum

    @staticmethod
    def _collect_imports(dag: HybridDAG) -> str:
        """Collect import statements from all nodes."""
        seen: set[str] = set()
        import_lines: list[str] = []
        for node in dag.nodes:
            for line in node.code.splitlines():
                stripped = line.strip()
                if (stripped.startswith("import ") or
                        stripped.startswith("from ")):
                    if stripped not in seen:
                        seen.add(stripped)
                        import_lines.append(line)
        return "\n".join(import_lines)

    @staticmethod
    def _build_code_for_type(dag: HybridDAG, step_type: StepType) -> str:
        """Concatenate code blocks of a given type in topological order."""
        ordered = dag.topological_order()
        parts: list[str] = []
        for node in ordered:
            if node.step_type == step_type:
                parts.append(f"# --- {node.label} ---")
                parts.append(node.code.strip())
                parts.append("")
        return "\n".join(parts)
