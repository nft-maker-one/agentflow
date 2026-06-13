"""Workflow-specific exceptions."""

from __future__ import annotations

from agentkit.common.errors import PermanentError, ValidationError


class CompileError(PermanentError):
    """Top-level error raised by the compiler when any step fails."""

    def __init__(self, message: str, *, violations: list[str] | None = None) -> None:
        super().__init__(message)
        self.violations: list[str] = violations or []

    def __str__(self) -> str:
        base = super().__str__()
        if not self.violations:
            return base
        lines = [base, "Violations:"]
        lines.extend(f"  - {v}" for v in self.violations)
        return "\n".join(lines)


class IRValidationError(ValidationError):
    """Raised by the validate step. Carries a list of violation strings."""

    def __init__(self, violations: list[str]) -> None:
        super().__init__(
            f"Workflow IR validation failed with {len(violations)} violation(s)",
        )
        self.violations = violations
