"""Workflow IR + Compiler — design-time central editor.

Public surface::

    from agentkit.workflow import (
        # IR data models
        WorkflowIR, AgentTemplate, EdgeSpec, FallbackSpec, Subscription,
        PublishSpec, WorkflowGuardrail, AgentGuardrail, RunGuardrail,
        ReplicaSpec, TriggerSpec, NotificationRule, EdgeBranch, Switch,
        # Compiler
        compile_workflow, compile_from_dict,
        CompileError, IRValidationError,
        # Plan products
        RuntimePlan, AgentPlan, BusTopicPlan,
        # YAML loading
        load_workflow_yaml,
    )

See ``Doc04_WorkflowGraph.md`` for the design.
"""

from agentkit.workflow.compiler import (
    compile_from_dict,
    compile_workflow,
)
from agentkit.workflow.errors import CompileError, IRValidationError
from agentkit.workflow.ir import (
    AgentGuardrail,
    AgentTemplate,
    EdgeBranch,
    EdgeSpec,
    FallbackSpec,
    IRMeta,
    NotificationRule,
    PublishSpec,
    ReplicaSpec,
    RunGuardrail,
    Subscription,
    Switch,
    TriggerSpec,
    WorkflowGuardrail,
    WorkflowIR,
)
from agentkit.workflow.plan import (
    AgentPlan,
    BusTopicPlan,
    BusTopicSpec,
    RuntimePlan,
)
from agentkit.workflow.yaml_loader import load_workflow_yaml

__all__ = [
    "AgentGuardrail",
    "AgentPlan",
    "AgentTemplate",
    "BusTopicPlan",
    "BusTopicSpec",
    "CompileError",
    "EdgeBranch",
    "EdgeSpec",
    "FallbackSpec",
    "IRMeta",
    "IRValidationError",
    "NotificationRule",
    "PublishSpec",
    "ReplicaSpec",
    "RunGuardrail",
    "RuntimePlan",
    "Subscription",
    "Switch",
    "TriggerSpec",
    "WorkflowGuardrail",
    "WorkflowIR",
    "compile_from_dict",
    "compile_workflow",
    "load_workflow_yaml",
]
