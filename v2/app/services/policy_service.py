"""Application service for reading/updating the AutomationPolicy."""
from ..domain.automation_policy import AutomationPolicy, validate_policy_input
from ..persistence.policy_repository import PolicyRepository


class PolicyService:
    def __init__(self, repository: PolicyRepository):
        self.repository = repository

    def get_policy(self) -> AutomationPolicy:
        return self.repository.get()

    def update_policy(self, data: dict) -> tuple[AutomationPolicy | None, list[str]]:
        errors = validate_policy_input(data, partial=True)
        if errors:
            return None, errors
        return self.repository.update(data), []

    def summary(self, policy: AutomationPolicy | None = None) -> str:
        policy = policy or self.get_policy()
        return policy.summary()
