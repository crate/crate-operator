from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict

from crate.operator.webhooks import WebhookEvent, WebhookStatus, WebhookSubPayload


class State(str, Enum):
    RESTART = "restart"
    SCALE = "scale"
    UPGRADE = "upgrade"


class StateMachine:
    """
    A simple, synchronous state machine implementation. Instantiated with a
    list of states, they are taken in order and can be stepped through forward.
    There is no backward stepping, because the state machine is used in the
    update handler for cluster resources and there is no "undo" functionality".
    There's only a way forward::

        >>> machine = StateMachine([State.UPGRADE, State.RESTART])
        >>> machine.current
        <State.UPGRADE: 'upgrade'>
        >>> machine.done
        False
        >>> machine.next()
        <State.UPGRADE: 'upgrade'>
        >>> machine.current
        <State.RESTART: 'restart'>
        >>> machine.done
        False
        >>> machine.next()
        <State.RESTART: 'restart'>
        >>> machine.current
        >>> machine.done
        True

    """

    def __init__(self, states: List[State] = None) -> None:
        self._states = [] if states is None else states

    def __repr__(self):
        return f"<{self.__class__.__name__}({self._states!r})>"

    def add(self, state: State) -> None:
        """
        Append a new :class:`State` to the state machine.
        """
        self._states.append(state)

    @property
    def current(self) -> Optional[State]:
        """
        Return the current :class:`State` or from the state machine or ``None``
        if there are no states left.
        """
        try:
            return self._states[0]
        except IndexError:
            return None

    @property
    def done(self) -> bool:
        """
        Return ``True`` if there are no states left in the state machine, else
        ``False``.
        """
        return len(self._states) == 0

    def next(self):
        """
        Return the current :class:`State` from the state machine and then
        remove it. If there are no states left, return ``None``.
        """
        try:
            return self._states.pop(0)
        except IndexError:
            return None

    def serialize(self) -> List[State]:
        return self._states


class Notification(TypedDict):
    event: WebhookEvent
    payload: WebhookSubPayload
    status: WebhookStatus


class Context:
    """
    The :class:`Context` is a place to store state between instances of
    :class:`~crate.operator.utils.kopf.StateBasedSubHandler`\\s.

    It is used to track the remaining states for the :class:`StateMachine` as
    well as notifications that
    :class:`~crate.operator.utils.kopf.StateBasedSubHandler`\\s may add or send
    out.

    .. note::

        The :class:`Context` is **not** meant as a place for individual
        sub-handlers to store their state! They should do that however works
        best for them.
    """

    def __init__(self) -> None:
        self._notifications: List[Notification] = []
        self.state_machine = StateMachine([])

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}("
            f"notifications={len(self._notifications)}, "
            f"state_machine={self.state_machine!r}"
            ")>"
        )

    @classmethod
    def deserialize(cls, context) -> "Context":
        ctx = Context()
        if context is not None:
            ctx._notifications = context["pendingNotifications"]
            ctx.state_machine = StateMachine(
                [State(s) for s in context["pendingStates"]]
            )
        return ctx

    def serialize(self) -> Dict[str, Any]:
        return {
            "pendingNotifications": self._notifications,
            "pendingStates": self.state_machine.serialize(),
        }

    def schedule_notification(
        self,
        event: WebhookEvent,
        payload: WebhookSubPayload,
        status: WebhookStatus,
    ):
        self._notifications.append(
            Notification(event=event, payload=payload, status=status)
        )

    @property
    def notifications(self) -> List[Notification]:
        return self._notifications

    def clear_notifications(self) -> None:
        self._notifications.clear()
