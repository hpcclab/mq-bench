use std::collections::HashSet;

/// Local sequence tracker for duplicate/gap detection
/// Each subscriber instance owns one of these to avoid shared state contention
#[derive(Debug)]
pub struct SequenceTracker {
    seen: HashSet<u64>,
    min_seq: Option<u64>,
    max_seq: u64,
    duplicate_count: u64,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            min_seq: None,
            max_seq: 0,
            duplicate_count: 0,
        }
    }

    /// Record a sequence number, returns true if new (not duplicate)
    #[inline]
    pub fn record(&mut self, seq: u64) -> bool {
        if self.seen.insert(seq) {
            // New sequence
            self.max_seq = self.max_seq.max(seq);
            match self.min_seq {
                None => self.min_seq = Some(seq),
                Some(m) if seq < m => self.min_seq = Some(seq),
                _ => {}
            }
            true
        } else {
            // Duplicate
            self.duplicate_count += 1;
            false
        }
    }

    /// Record a batch of sequences, returns count of new (non-duplicate) messages
    #[inline]
    pub fn record_batch(&mut self, seqs: &[u64]) -> usize {
        let mut new_count = 0;
        for &seq in seqs {
            if self.record(seq) {
                new_count += 1;
            }
        }
        new_count
    }

    /// Get count of duplicate messages seen
    #[inline]
    pub fn duplicate_count(&self) -> u64 {
        self.duplicate_count
    }

    /// Get count of gaps (missing sequences between min and max)
    /// Note: This only counts gaps WITHIN the received range, not messages
    /// lost before the first received message or after the last.
    #[inline]
    pub fn gap_count(&self) -> u64 {
        if let Some(min) = self.min_seq {
            if self.max_seq >= min {
                let expected = self.max_seq - min + 1;
                let actual = self.seen.len() as u64;
                return expected.saturating_sub(actual);
            }
        }
        0
    }

    /// Get count of messages lost at the start (before first received sequence)
    /// This is the min_seq value (assuming sequences start at 0)
    #[inline]
    pub fn head_loss(&self) -> u64 {
        self.min_seq.unwrap_or(0)
    }

    /// Get total unique messages received
    #[inline]
    pub fn unique_count(&self) -> u64 {
        self.seen.len() as u64
    }

    /// Get min sequence seen
    #[inline]
    pub fn min_seq(&self) -> Option<u64> {
        self.min_seq
    }

    /// Get max sequence seen
    #[inline]
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Reset the tracker
    pub fn reset(&mut self) {
        self.seen.clear();
        self.min_seq = None;
        self.max_seq = 0;
        self.duplicate_count = 0;
    }
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sequences() {
        let mut tracker = SequenceTracker::new();
        assert!(tracker.record(1));
        assert!(tracker.record(2));
        assert!(tracker.record(3));
        assert_eq!(tracker.unique_count(), 3);
        assert_eq!(tracker.duplicate_count(), 0);
        assert_eq!(tracker.gap_count(), 0);
    }

    #[test]
    fn test_duplicates() {
        let mut tracker = SequenceTracker::new();
        assert!(tracker.record(1));
        assert!(!tracker.record(1)); // duplicate
        assert!(tracker.record(2));
        assert!(!tracker.record(1)); // duplicate again
        assert_eq!(tracker.unique_count(), 2);
        assert_eq!(tracker.duplicate_count(), 2);
    }

    #[test]
    fn test_gaps() {
        let mut tracker = SequenceTracker::new();
        tracker.record(1);
        tracker.record(5); // gap of 3 (missing 2,3,4)
        assert_eq!(tracker.gap_count(), 3);
        
        tracker.record(3); // fill one gap
        assert_eq!(tracker.gap_count(), 2);
    }

    #[test]
    fn test_batch() {
        let mut tracker = SequenceTracker::new();
        let new_count = tracker.record_batch(&[1, 2, 3, 2, 4, 1]); // 2 duplicates
        assert_eq!(new_count, 4);
        assert_eq!(tracker.duplicate_count(), 2);
        assert_eq!(tracker.unique_count(), 4);
    }
}
