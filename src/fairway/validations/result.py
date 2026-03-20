"""Validation result model for fairway data quality checks."""
from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    """Aggregated result of all validation checks on a dataset.

    Findings are categorized as errors (block pipeline) or warnings (log only).
    Threshold support: if a finding has a threshold and the violation rate is
    below it, the finding is downgraded from error to warning.
    """
    passed: bool = True
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    def add_finding(self, finding: dict):
        """Add a validation finding, applying threshold logic if present.

        Args:
            finding: dict with keys: column, check, message, severity,
                     failed_count, total_count, and optionally threshold.
        """
        severity = finding.get("severity", "error")
        threshold = finding.get("threshold")

        # Threshold downgrade: if violation rate is below threshold,
        # treat error as warning
        if threshold is not None and severity == "error":
            total = finding.get("total_count", 0)
            failed = finding.get("failed_count", 0)
            if total > 0 and (failed / total) < threshold:
                severity = "warn"

        if severity == "error":
            self.errors.append(finding)
            self.passed = False
        else:
            self.warnings.append(finding)
