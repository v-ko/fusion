from fusion.libs.entity.change import Change
from fusion.libs.entity.delta import Delta
from fusion.logging import get_logger

log = get_logger(__name__)


class ChangeAggregator:
    """
    Merges the changes passed to the input_channel and when a message is
    received on the release_trigger_channel - pushes all accumulated changes
    to the output_channel or changeset_output_channel (or both if set).
    The difference between the latter two is that on the changeset channel
    all of the changes are sent as a list as a single message.

    Internally accumulates changes into a Delta, merging per-entity changes
    using the Delta merge rules.
    """

    def __init__(
        self,
        input_channel,
        release_trigger_channel,
        output_channel=None,
        changeset_output_channel=None,
    ):
        self._delta = Delta()

        self.input_channel = input_channel
        self.release_trigger_channel = release_trigger_channel
        self.output_channel = output_channel
        self.changeset_output_channel = changeset_output_channel

        self.raw_sub_id = input_channel.subscribe(self.handle_change)
        self.actions_sub_id = release_trigger_channel.subscribe(
            self.release_aggregated_changes
        )

    def handle_change(self, change: Change):
        """Accumulate a change into the internal Delta."""
        self._delta.add_change_from_data(change.data)

    def release_aggregated_changes(self, completed_actions):
        changes = list(self._delta.changes())
        self._delta = Delta()

        if not changes:
            log.info("RELEASE_AGGREGATED_CHANGES: No changes")
            return

        log.info("RELEASE_AGGREGATED_CHANGES:")

        if self.output_channel:
            for change in changes:
                self.output_channel.push(change)

        if self.changeset_output_channel:
            self.changeset_output_channel.push(changes)
