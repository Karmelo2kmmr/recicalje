use log::info;
use std::collections::VecDeque;

/// Volume tracker for detecting volume spikes to prevent false SL triggers
#[derive(Debug, Clone)]
pub struct VolumeTracker {
    /// Rolling window of volume measurements (last 30 ticks)
    volume_history: VecDeque<f64>,
    /// Maximum history size
    max_history: usize,
    /// Multiplier for spike detection (e.g., 3.0 = 3x average)
    spike_multiplier: f64,
}

impl VolumeTracker {
    /// Create a new volume tracker
    pub fn new() -> Self {
        let spike_multiplier = std::env::var("VOLUME_SPIKE_MULT")
            .unwrap_or("3.0".to_string())
            .parse()
            .unwrap_or(3.0);

        Self {
            volume_history: VecDeque::with_capacity(30),
            max_history: 30,
            spike_multiplier,
        }
    }

    /// Add a new volume measurement
    pub fn add_volume(&mut self, volume: f64) {
        if self.volume_history.len() >= self.max_history {
            self.volume_history.pop_front();
        }
        self.volume_history.push_back(volume);
    }

    /// Calculate average volume from history
    fn average_volume(&self) -> f64 {
        if self.volume_history.is_empty() {
            return 0.0;
        }

        let sum: f64 = self.volume_history.iter().sum();
        sum / self.volume_history.len() as f64
    }

    /// Check if current volume represents a spike
    ///
    /// Returns true if current_volume >= average * spike_multiplier
    pub fn is_spike(&self, current_volume: f64) -> bool {
        if self.volume_history.len() < 5 {
            // Not enough history, can't determine spike
            return false;
        }

        let avg = self.average_volume();
        if avg <= 0.0 {
            return false;
        }

        let is_spike = current_volume >= avg * self.spike_multiplier;

        if is_spike {
            info!(
                "🔊 VOLUME SPIKE DETECTED! Current: {:.2} | Avg: {:.2} | Ratio: {:.2}x",
                current_volume,
                avg,
                current_volume / avg
            );
        }

        is_spike
    }

    /// Get current average volume
    pub fn get_average(&self) -> f64 {
        self.average_volume()
    }

    /// Get number of samples in history
    pub fn sample_count(&self) -> usize {
        self.volume_history.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_tracker_basic() {
        let mut tracker = VolumeTracker::new();

        // Add baseline volumes
        for _ in 0..10 {
            tracker.add_volume(100.0);
        }

        assert_eq!(tracker.average_volume(), 100.0);
        assert!(!tracker.is_spike(200.0)); // 2x is not a spike (need 3x)
        assert!(tracker.is_spike(300.0)); // 3x is a spike
    }

    #[test]
    fn test_volume_tracker_rolling_window() {
        let mut tracker = VolumeTracker::new();

        // Fill beyond max capacity
        for i in 0..40 {
            tracker.add_volume(i as f64);
        }

        // Should only keep last 30
        assert_eq!(tracker.sample_count(), 30);
    }

    #[test]
    fn test_insufficient_history() {
        let mut tracker = VolumeTracker::new();

        // Only 3 samples
        tracker.add_volume(100.0);
        tracker.add_volume(100.0);
        tracker.add_volume(100.0);

        // Should not detect spike with insufficient history
        assert!(!tracker.is_spike(500.0));
    }
}
