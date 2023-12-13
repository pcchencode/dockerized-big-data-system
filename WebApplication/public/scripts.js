document.getElementById('fetch-track').addEventListener('click', function() {
    const trackId = document.getElementById('track-id-input').value.trim(); // Added trim() to remove whitespace
    fetch(`/api/tracks/${trackId}`)
    .then(response => response.json())
    .then(track => {
        if (track) {
        const detailsDiv = document.getElementById('track-details');

        // Set the genre and key values
        document.getElementById('genre-value').innerHTML = track.track_genre || 'N/A';
        document.getElementById('key-value').innerHTML = track.key || 'N/A';

        detailsDiv.innerHTML = `
            <p>Artist: ${track.artists}</p>
            <p>Album Name: ${track.album_name}</p>
            <p>Track Name: ${track.track_id}</p>
            <p>Duration: ${track.duration}</p>
            <!-- Add more attributes here -->
        `;
        // Now call the function to render the chart
        fetchTrackDataAndRenderChart(trackId);
        // Now update the popularity meter
        updatePopularityMeter(data.popularity);
        } else {
        document.getElementById('track-details').innerHTML = 'Track not found.';
        }
    })
    .catch(error => console.error('Error:', error));
});

function updatePopularityMeter(popularity) {
  const popularityMeter = document.getElementById('popularity-meter');
  const popularityScoreElement = document.getElementById('popularity-score');

  // Assume the popularity is a number from 0 to 100
  // Convert it to a percentage of the meter's height if necessary
  const meterHeight = popularityMeter.offsetHeight; // Get the height of the meter
  const popularityHeight = (popularity / 100) * meterHeight; // Calculate the height for the score

  // Set the height of the score element
  popularityScoreElement.style.height = `${popularityHeight}px`;

  // Set the text inside the score element
  popularityScoreElement.textContent = popularity;

  // Adjust the position of the score element to fill from the bottom
  popularityScoreElement.style.bottom = '0';
}

// Declare a variable outside of your function to hold the chart instance
let audioFeaturesChart;

function renderAudioFeaturesChart(features) {
    const ctx = document.getElementById('audioFeaturesChart').getContext('2d');

    // Check if the chart instance exists
    if (audioFeaturesChart) {
    audioFeaturesChart.destroy(); // Destroy the existing chart instance
    }

    audioFeaturesChart = new Chart(ctx, {
      type: 'radar',
      data: {
        labels: ['Danceability', 'Energy', 'Loudness', 'Liveness', 'Acousticness', 'Tempo'],
        datasets: [{
          label: 'Audio Features',
          data: [
            features.danceability,
            features.energy,
            features.loudness,
            features.liveness,
            features.acousticne,
            features.tempo,
          ],
          backgroundColor: 'rgba(255, 99, 132, 0.2)', // Semi-transparent background
          borderColor: 'rgba(255, 99, 132, 1)', // Line color
          pointBackgroundColor: 'rgba(255, 99, 132, 1)', // Point color
          pointBorderColor: '#fff', // Point border color
          pointHoverBackgroundColor: '#fff', // Hover color
          pointHoverBorderColor: 'rgba(255, 99, 132, 1)', // Hover border color
        }]
      },
      options: {
        responsive: true, // Make it responsive
        maintainAspectRatio: true, // Maintain the aspect ratio
        aspectRatio: 1,

        elements: {
          line: {
            tension: 0, // Straight lines
            borderWidth: 3 // Line thickness
          },
          point: {
            radius: 5, // Point size
            hitRadius: 10, // Clickable area size
            hoverRadius: 7 // Hover state size
          }
        },
        scale: {
          ticks: {
            beginAtZero: true,
            backdropColor: 'transparent', // Ticks background
          },
          gridLines: {
            color: 'rgba(255, 255, 255, 0.5)' // Grid lines color
          },
          angleLines: {
            color: 'rgba(255, 255, 255, 0.5)' // Angle lines color
          }
        },
        animation: {
          duration: 1500, // Animation duration
          easing: 'easeOutElastic' // Animation transition
        },
        legend: {
          position: 'top', // Legend position
        },
        tooltips: {
          enabled: true,
          backgroundColor: 'rgba(0,0,0,0.7)', // Tooltip background color
          bodyFontColor: 'white', // Tooltip text color
          borderColor: 'rgba(0,0,0,0.7)', // Tooltip border color
          borderWidth: 1 // Tooltip border width
        }
      }
    });
  }
  

// Assume this function is called when the user submits a form with the track ID
function fetchTrackDataAndRenderChart(trackId) {
    fetch(`/api/tracks/${trackId}`)
      .then(response => response.json())
      .then(data => {
        // Assuming 'data' is an object with keys that match your audio features
        const features = {
          danceability: data.danceability,
          energy: data.energy,
          loudness: data.loudness,
          liveness: data.liveness,
          acousticne: data.acousticne,
          tempo: data.tempo,
        };
        renderAudioFeaturesChart(features);

        // Update the popularity meter
        const popularityScoreElement = document.getElementById('popularity-score');
        // Assume popularity is a number from 0 to 100
        const popularityPercentage = data.popularity; // If popularity is not already a percentage, calculate it
        popularityScoreElement.style.height = `${popularityPercentage}%`;
        popularityScoreElement.textContent = data.popularity; // Display the popularity number

      })
      .catch(error => console.error('Error:', error));
  }
  
  
