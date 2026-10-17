//! `ServiceAck` and the per-service queues (`ServiceAck.java`).
//!
//! Every service answers a `JoinLog` (and later a snapshot, and a termination)
//! with an ack carrying the same `(logPosition, ackId)` it was asked about. The
//! queues hold one answer per service; "reached" means **every** service's head
//! matches exactly, and an answer out of sequence is an invariant failure, not
//! something to skip.

use std::collections::VecDeque;

use crate::error::ClusterError;

/// `Aeron.NULL_VALUE`.
pub const NULL_VALUE: i64 = -1;

/// One service's answer (`ServiceAck.java:29-57`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceAck {
    ack_id: i64,
    log_position: i64,
    relevant_id: i64,
}

impl ServiceAck {
    /// `new ServiceAck(ackId, logPosition, relevantId)`.
    pub const fn new(ack_id: i64, log_position: i64, relevant_id: i64) -> Self {
        Self {
            ack_id,
            log_position,
            relevant_id,
        }
    }

    /// The ack's sequence number.
    pub const fn ack_id(&self) -> i64 {
        self.ack_id
    }

    /// The log position the service had applied.
    pub const fn log_position(&self) -> i64 {
        self.log_position
    }

    /// The service's own relevant id: its snapshot's recording id, or
    /// [`NULL_VALUE`].
    pub const fn relevant_id(&self) -> i64 {
        self.relevant_id
    }
}

/// `ArrayDeque<ServiceAck>[]`, one per service.
pub type ServiceAckQueues = Vec<VecDeque<ServiceAck>>;

/// `ServiceAck.newArrayOfQueues(serviceCount)` (`:146-157`).
pub fn new_array_of_queues(service_count: usize) -> ServiceAckQueues {
    (0..service_count).map(|_| VecDeque::new()).collect()
}

/// `ServiceAck.hasReached(logPosition, ackId, queues)` (`:59-79`).
///
/// # Errors
///
/// [`ClusterError::Cluster`] with the reference's `ack out of sequence` words
/// when a queue's head names a different ack id or position.
pub fn has_reached(
    log_position: i64,
    ack_id: i64,
    queues: &ServiceAckQueues,
) -> Result<bool, ClusterError> {
    for (service_id, queue) in queues.iter().enumerate() {
        let Some(head) = queue.front() else {
            return Ok(false);
        };

        if head.ack_id != ack_id || head.log_position != log_position {
            return Err(ClusterError::Cluster(format!(
                "ack out of sequence: expected [ackId={ack_id}, logPosition={log_position}] vs \
                 received [ackId={}, logPosition={}, relevantId={}, serviceId={}]",
                head.ack_id, head.log_position, head.relevant_id, service_id
            )));
        }
    }
    Ok(true)
}

/// `ServiceAck.removeHead(queues)` (`:81-87`).
pub fn remove_head(queues: &mut ServiceAckQueues) {
    for queue in queues.iter_mut() {
        queue.pop_front();
    }
}

/// `ServiceAck.pollServiceAcks(queues)` (`:89-104`).
///
/// # Errors
///
/// [`ClusterError::Cluster`] with `invalid ack for serviceId=` when a queue is
/// empty.
pub fn poll_service_acks(queues: &mut ServiceAckQueues) -> Result<Vec<ServiceAck>, ClusterError> {
    let mut acks = Vec::with_capacity(queues.len());
    for (service_id, queue) in queues.iter_mut().enumerate() {
        let Some(ack) = queue.pop_front() else {
            return Err(ClusterError::Cluster(format!(
                "invalid ack for serviceId={service_id}"
            )));
        };
        acks.push(ack);
    }
    Ok(acks)
}

/// `ServiceAck.pollServiceAcks(logPosition, serviceId, queues)` (`:106-125`).
///
/// # Errors
///
/// [`ClusterError::Cluster`] when the queue is empty or the head names another
/// position.
pub fn poll_service_acks_at(
    log_position: i64,
    service_id: usize,
    queues: &mut ServiceAckQueues,
) -> Result<Vec<ServiceAck>, ClusterError> {
    let mut acks = Vec::with_capacity(queues.len());
    for (index, queue) in queues.iter_mut().enumerate() {
        let Some(ack) = queue.pop_front() else {
            return Err(ClusterError::Cluster(format!(
                "invalid ack for serviceId={index}"
            )));
        };
        if index == service_id && ack.log_position() != log_position {
            return Err(ClusterError::Cluster(format!(
                "invalid ack for serviceId={service_id} logPosition={log_position}"
            )));
        }
        acks.push(ack);
    }
    Ok(acks)
}

/// `ServiceAck.areAllRelevantIdsNonNull(message, serviceAcks, errorLog)`
/// (`:127-144`): the error lines the reference would record for null relevant
/// ids, highest service id first.
pub fn null_relevant_ids(message: &str, acks: &[ServiceAck]) -> Vec<String> {
    let mut errors = Vec::new();
    for (service_id, ack) in acks.iter().enumerate().rev() {
        if ack.relevant_id() == NULL_VALUE {
            errors.push(format!("service={service_id} {message}"));
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_queue_must_hold_the_exact_head() {
        let mut queues = new_array_of_queues(2);
        queues[0].push_back(ServiceAck::new(5, 100, 0));
        assert!(
            !has_reached(100, 5, &queues).unwrap(),
            "the second queue is empty"
        );

        queues[1].push_back(ServiceAck::new(5, 100, 1));
        assert!(has_reached(100, 5, &queues).unwrap());

        queues[1].back_mut().unwrap().ack_id = 4;
        let error = has_reached(100, 5, &queues).unwrap_err();
        assert!(error.to_string().contains("ack out of sequence"));
    }

    #[test]
    fn a_null_relevant_id_is_reported_from_the_highest_service_down() {
        let acks = vec![
            ServiceAck::new(0, 0, NULL_VALUE),
            ServiceAck::new(0, 0, 7),
            ServiceAck::new(0, 0, NULL_VALUE),
        ];
        assert_eq!(
            vec![
                "service=2 failed to take snapshot".to_owned(),
                "service=0 failed to take snapshot".to_owned()
            ],
            null_relevant_ids("failed to take snapshot", &acks)
        );
    }
}
