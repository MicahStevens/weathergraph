// Global variables
let currentData = null;
let currentMonthData = null;

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    initializeDateOptions();
    refreshCacheStats();
    
    // Add input validation
    document.getElementById('zipCode').addEventListener('input', validateZipCode);
    document.getElementById('startYear').addEventListener('change', validateYearRange);
    document.getElementById('endYear').addEventListener('change', validateYearRange);
    document.getElementById('startMonth').addEventListener('change', updateDayOptions);
    document.getElementById('endMonth').addEventListener('change', updateDayOptions);
    
    // Add listeners for temperature series toggles and threshold controls
    document.getElementById('thresholdTemp').addEventListener('change', function() {
        if (currentData) {
            plotTemperatureData(currentData);
        }
    });
    
    // Add listeners for temperature series checkboxes
    const tempToggles = document.querySelectorAll('.temp-toggle');
    tempToggles.forEach(toggle => {
        toggle.addEventListener('change', function() {
            if (currentData) {
                plotTemperatureData(currentData);
            }
        });
    });
    
    // Set default days to 1st
    document.getElementById('startDay').value = '1';
    document.getElementById('endDay').value = '1';
});

// Initialize day options based on selected months
function initializeDateOptions() {
    updateDayOptions();
}

// Update day options based on selected months
function updateDayOptions() {
    const startMonth = parseInt(document.getElementById('startMonth').value);
    const endMonth = parseInt(document.getElementById('endMonth').value);
    const startDaySelect = document.getElementById('startDay');
    const endDaySelect = document.getElementById('endDay');
    
    const currentStartDay = startDaySelect.value || '1';
    const currentEndDay = endDaySelect.value || '1';
    
    // Days in each month (assuming non-leap year for February)
    const daysInMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    const startMaxDays = daysInMonth[startMonth - 1];
    const endMaxDays = daysInMonth[endMonth - 1];
    
    // Update start day options
    startDaySelect.innerHTML = '';
    for (let day = 1; day <= startMaxDays; day++) {
        const option = document.createElement('option');
        option.value = day;
        option.textContent = day;
        if (day == currentStartDay && day <= startMaxDays) {
            option.selected = true;
        }
        startDaySelect.appendChild(option);
    }
    
    // Update end day options
    endDaySelect.innerHTML = '';
    for (let day = 1; day <= endMaxDays; day++) {
        const option = document.createElement('option');
        option.value = day;
        option.textContent = day;
        if (day == currentEndDay && day <= endMaxDays) {
            option.selected = true;
        }
        endDaySelect.appendChild(option);
    }
    
    // Adjust if current day is invalid for the new month
    if (parseInt(currentStartDay) > startMaxDays) {
        startDaySelect.value = '1';
    }
    if (parseInt(currentEndDay) > endMaxDays) {
        endDaySelect.value = '1';
    }
}

// Validate ZIP code input
function validateZipCode() {
    const zipInput = document.getElementById('zipCode');
    const zipCode = zipInput.value.trim();
    
    // Remove non-numeric characters
    zipInput.value = zipCode.replace(/\D/g, '');
    
    // Limit to 5 digits
    if (zipInput.value.length > 5) {
        zipInput.value = zipInput.value.slice(0, 5);
    }
}

// Validate year range
function validateYearRange() {
    const startYear = parseInt(document.getElementById('startYear').value);
    const endYear = parseInt(document.getElementById('endYear').value);
    
    if (startYear > endYear) {
        showStatus('End year must be greater than or equal to start year', 'error');
        document.getElementById('endYear').value = startYear;
    }
}

// Show status message
function showStatus(message, type = 'info') {
    const statusElement = document.getElementById('statusMessage');
    statusElement.textContent = message;
    statusElement.className = `status-message ${type}`;
    statusElement.style.display = 'block';
    
    // Auto-hide after 5 seconds for success/info messages
    if (type !== 'error') {
        setTimeout(() => {
            statusElement.style.display = 'none';
        }, 5000);
    }
}

// Toggle loading state for fetch button
function setLoading(isLoading) {
    const button = document.getElementById('fetchData');
    const buttonText = document.getElementById('buttonText');
    const spinner = document.getElementById('loadingSpinner');
    
    button.disabled = isLoading;
    
    if (isLoading) {
        buttonText.textContent = 'Fetching Data...';
        spinner.style.display = 'inline';
    } else {
        buttonText.textContent = 'Fetch Weather Data';
        spinner.style.display = 'none';
    }
}

// Main function to fetch weather data
async function fetchWeatherData() {
    const zipCode = document.getElementById('zipCode').value.trim();
    const startMonth = parseInt(document.getElementById('startMonth').value);
    const startDay = parseInt(document.getElementById('startDay').value);
    const endMonth = parseInt(document.getElementById('endMonth').value);
    const endDay = parseInt(document.getElementById('endDay').value);
    const startYear = parseInt(document.getElementById('startYear').value);
    const endYear = parseInt(document.getElementById('endYear').value);
    
    // Validation
    if (!zipCode || zipCode.length !== 5) {
        showStatus('Please enter a valid 5-digit ZIP code', 'error');
        return;
    }
    
    if (startYear > endYear) {
        showStatus('Start year must be less than or equal to end year', 'error');
        return;
    }
    
    setLoading(true);
    showStatus('Fetching weather data... This may take a moment.', 'info');
    
    try {
         // Call Python backend through Eel
         const result = await eel.get_weather_data(zipCode, startMonth, startDay, endMonth, endDay, startYear, endYear)();
         
         if (result.success) {
             currentData = result.data;
             currentMonthData = result.daily_data || [];
             
             console.log('Fetched aggregated data with', result.data.length, 'records');
             console.log('Fetched daily data with', currentMonthData.length, 'records');
             
             plotTemperatureData(result.data);
             showStatus(`Successfully loaded weather data for ${result.location_name || zipCode}`, 'success');
         } else {
             showStatus(`Error: ${result.error}`, 'error');
             clearPlot();
         }
         
     } catch (error) {
         console.error('Error fetching weather data:', error);
         showStatus('Failed to fetch weather data. Please check your internet connection and try again.', 'error');
         clearPlot();
     } finally {
         setLoading(false);
         refreshCacheStats();
     }
}

// Calculate moving average
function calculateMovingAverage(values, windowSize) {
    const result = [];
    for (let i = 0; i < values.length; i++) {
        const start = Math.max(0, i - Math.floor(windowSize / 2));
        const end = Math.min(values.length, i + Math.floor(windowSize / 2) + 1);
        const window = values.slice(start, end);
        const avg = window.reduce((sum, val) => sum + (val || 0), 0) / window.length;
        result.push(avg);
    }
    return result;
}

// Calculate days above a given temperature threshold for each year
function calculateDaysAboveThreshold(monthData, threshold) {
    // Group month data by year and count days above threshold for each year
    const daysByYear = {};
    
    if (!monthData || monthData.length === 0) {
        console.warn('No month data available for threshold calculation');
        return [];
    }
    
    console.log('Calculating days above threshold. Data points:', monthData.length, 'Threshold:', threshold);
    console.log('First record sample:', monthData[0]);
    
    // Initialize year counts
    monthData.forEach(record => {
        if (!daysByYear[record.year]) {
            daysByYear[record.year] = 0;
        }
        
        const maxTemp = parseFloat(record.temperature_2m_max);
        
        // Debug: log when we find data
        if (!isNaN(maxTemp)) {
            if (maxTemp >= threshold) {
                daysByYear[record.year]++;
            }
        } else {
            console.warn('Invalid or missing temperature_2m_max for record:', record);
        }
    });
    
    console.log('Days by year:', daysByYear);
    
    // Get sorted years and map to array values
    const sortedYears = Object.keys(daysByYear).map(Number).sort((a, b) => a - b);
    return sortedYears.map(year => daysByYear[year]);
}

// Plot temperature data using Plotly
function plotTemperatureData(data) {
    console.log('plotTemperatureData called with data:', data);
    
    if (!data || data.length === 0) {
        showStatus('No data available for the selected parameters', 'error');
        clearPlot();
        return;
    }
    
    try {
         const years = data.map(d => d.year);
         
         // Handle both single-day and date-range aggregated data
         // For date ranges, we have _mean, _min, _max fields; for single days, we have direct fields
         const maxTemps = data.map(d => d.temperature_2m_max_max || d.temperature_2m_max);
         const minTemps = data.map(d => d.temperature_2m_min_min || d.temperature_2m_min);
         const avgTemps = data.map(d => d.temperature_2m_mean_mean || d.temperature_2m_mean);
         const meanTemps = data.map(d => d.temperature_2m_mean_mean || d.temperature_2m_mean);
         
         console.log('Data mapped - years:', years.length, 'maxTemps:', maxTemps.length, 'minTemps:', minTemps.length, 'avgTemps:', avgTemps.length, 'meanTemps:', meanTemps.length);
    
        // Get moving average window from user selection
        const movingAvgElement = document.getElementById('movingAvgWindow');
        let windowSize;
        
        if (movingAvgElement) {
            const movingAvgSelect = movingAvgElement.value;
            if (movingAvgSelect === 'auto') {
                windowSize = Math.max(3, Math.min(10, Math.floor(years.length / 4)));
            } else {
                windowSize = Math.min(parseInt(movingAvgSelect), Math.floor(years.length / 2));
            }
        } else {
            windowSize = Math.min(5, Math.floor(years.length / 3));
        }
        
        const maxTempMovingAvg = calculateMovingAverage(maxTemps, windowSize);
        const minTempMovingAvg = calculateMovingAverage(minTemps, windowSize);
        const avgTempMovingAvg = calculateMovingAverage(avgTemps, windowSize);
        const meanTempMovingAvg = calculateMovingAverage(meanTemps, windowSize);
        
        // Get user preferences for which series to show
        const showMax = document.getElementById('showMax').checked;
        const showMin = document.getElementById('showMin').checked;
        const showAvg = document.getElementById('showAvg').checked;
        const showMean = document.getElementById('showMean').checked;
        const showThresholdDays = document.getElementById('showThresholdDays').checked;
        const thresholdTemp = parseFloat(document.getElementById('thresholdTemp').value);
        
        const traces = [];
        
        // Add temperature traces based on user selection
        if (showMax) {
            traces.push({
                x: years,
                y: maxTemps,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Max Temperature',
                line: { color: '#FF4444', width: 2 },
                marker: { color: '#FF4444', size: 4 },
                opacity: 0.7,
                yaxis: 'y',
                hovertemplate: '<b>Max Temperature</b><br>Year: %{x}<br>Temperature: %{y:.1f}°F<extra></extra>'
            });
            
            traces.push({
                x: years,
                y: maxTempMovingAvg,
                type: 'scatter',
                mode: 'lines',
                name: `Max Temp Trend (${windowSize}-year avg)`,
                line: { color: '#CC0000', width: 4, smoothing: 0.8 },
                yaxis: 'y',
                hovertemplate: '<b>Max Temperature Trend</b><br>Year: %{x}<br>Avg Temperature: %{y:.1f}°F<extra></extra>'
            });
        }
        
        if (showMin) {
            traces.push({
                x: years,
                y: minTemps,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Min Temperature',
                line: { color: '#4444FF', width: 2 },
                marker: { color: '#4444FF', size: 4 },
                opacity: 0.7,
                yaxis: 'y',
                hovertemplate: '<b>Min Temperature</b><br>Year: %{x}<br>Temperature: %{y:.1f}°F<extra></extra>'
            });
            
            traces.push({
                x: years,
                y: minTempMovingAvg,
                type: 'scatter',
                mode: 'lines',
                name: `Min Temp Trend (${windowSize}-year avg)`,
                line: { color: '#0000CC', width: 4, smoothing: 0.8 },
                yaxis: 'y',
                hovertemplate: '<b>Min Temperature Trend</b><br>Year: %{x}<br>Avg Temperature: %{y:.1f}°F<extra></extra>'
            });
        }
        
        if (showAvg) {
            traces.push({
                x: years,
                y: avgTemps,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Average Temperature',
                line: { color: '#FF9900', width: 2 },
                marker: { color: '#FF9900', size: 4 },
                opacity: 0.7,
                yaxis: 'y',
                hovertemplate: '<b>Average Temperature</b><br>Year: %{x}<br>Temperature: %{y:.1f}°F<extra></extra>'
            });
            
            traces.push({
                x: years,
                y: avgTempMovingAvg,
                type: 'scatter',
                mode: 'lines',
                name: `Avg Temp Trend (${windowSize}-year avg)`,
                line: { color: '#FF6600', width: 4, smoothing: 0.8 },
                yaxis: 'y',
                hovertemplate: '<b>Average Temperature Trend</b><br>Year: %{x}<br>Avg Temperature: %{y:.1f}°F<extra></extra>'
            });
        }
        
        if (showMean) {
            traces.push({
                x: years,
                y: meanTemps,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Mean Temperature',
                line: { color: '#00AA88', width: 2 },
                marker: { color: '#00AA88', size: 4 },
                opacity: 0.7,
                yaxis: 'y',
                hovertemplate: '<b>Mean Temperature</b><br>Year: %{x}<br>Temperature: %{y:.1f}°F<extra></extra>'
            });
            
            traces.push({
                x: years,
                y: meanTempMovingAvg,
                type: 'scatter',
                mode: 'lines',
                name: `Mean Temp Trend (${windowSize}-year avg)`,
                line: { color: '#009966', width: 4, smoothing: 0.8 },
                yaxis: 'y',
                hovertemplate: '<b>Mean Temperature Trend</b><br>Year: %{x}<br>Mean Temperature: %{y:.1f}°F<extra></extra>'
            });
        }
        
        // Add days above threshold trace if enabled
        if (showThresholdDays) {
            if (!currentMonthData || currentMonthData.length === 0) {
                showStatus('Month data not available for threshold analysis', 'warning');
                console.error('currentMonthData is empty or undefined:', currentMonthData);
            } else {
                console.log('Processing threshold with', currentMonthData.length, 'daily records');
                const daysAboveThreshold = calculateDaysAboveThreshold(currentMonthData, thresholdTemp);
                console.log('Days above threshold result:', daysAboveThreshold);
                const daysAboveTrend = calculateMovingAverage(daysAboveThreshold, Math.max(3, Math.floor(windowSize / 2)));
                
                // Get the years from month data to align with the days count
                const monthDataByYear = {};
                currentMonthData.forEach(record => {
                    if (!monthDataByYear[record.year]) {
                        monthDataByYear[record.year] = true;
                    }
                });
                const monthYears = Object.keys(monthDataByYear).map(Number).sort((a, b) => a - b);
                
                traces.push({
                    x: monthYears,
                    y: daysAboveThreshold,
                    type: 'scatter',
                    mode: 'markers',
                    name: `Days Above ${thresholdTemp}°F`,
                    marker: { color: '#22AA00', size: 6 },
                    opacity: 0.6,
                    yaxis: 'y2',
                    hovertemplate: '<b>Days Above Threshold</b><br>Year: %{x}<br>Days Above ${thresholdTemp}°F: %{y}<extra></extra>'
                });
                
                traces.push({
                    x: monthYears,
                    y: daysAboveTrend,
                    type: 'scatter',
                    mode: 'lines',
                    name: `Days Above Threshold Trend`,
                    line: { color: '#009900', width: 3, smoothing: 0.8 },
                    yaxis: 'y2',
                    hovertemplate: '<b>Days Above Threshold Trend</b><br>Year: %{x}<br>Trend: %{y:.2f}<extra></extra>'
                });
            }
        }
        
        // Layout configuration with optional secondary Y-axis
        const layout = {
            title: {
                text: `Temperature Trends with ${windowSize}-Year Moving Average<br><sub>Date: ${getDateString()} | Period: ${years[0]} - ${years[years.length-1]}</sub>`,
                font: { size: 18, family: 'Segoe UI' }
            },
            xaxis: {
                title: 'Year',
                tickformat: 'd',
                dtick: Math.max(1, Math.floor((years[years.length-1] - years[0]) / 10)),
                gridcolor: '#E1E8ED'
            },
            yaxis: {
                title: 'Temperature (°F)',
                gridcolor: '#E1E8ED',
                side: 'left'
            },
            hovermode: 'x unified',
            legend: {
                x: 0,
                y: -0.15,
                orientation: 'h',
                xanchor: 'left'
            },
            plot_bgcolor: '#FAFBFC',
            paper_bgcolor: 'white',
            margin: { l: 50, r: showThresholdDays ? 80 : 20, t: 80, b: 100 },
            autosize: true
        };
        
        // Add secondary Y-axis if threshold days are shown
        if (showThresholdDays) {
            layout.yaxis2 = {
                title: 'Days Count',
                overlaying: 'y',
                side: 'right',
                gridcolor: 'rgba(255,153,0,0.2)'
            };
        }
        
        const config = {
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
            displaylogo: false,
            toImageButtonOptions: {
                format: 'png',
                filename: `weather_trends_${getDateString()}_${years[0]}-${years[years.length-1]}_${windowSize}yr_avg`,
                height: 700,
                width: 1200,
                scale: 1
            }
        };
        
        const plotDiv = document.getElementById('temperaturePlot');
        console.log('Creating plot in div:', plotDiv, 'with traces:', traces.length);
        
        plotDiv.style.display = 'block';
        plotDiv.style.alignItems = '';
        plotDiv.style.justifyContent = '';
        
        Plotly.newPlot(plotDiv, traces, layout, config).then(() => {
            console.log('Plot created successfully');
            
            plotDiv.style.height = '500px';
            plotDiv.style.minHeight = '500px';
            plotDiv.style.display = 'block';
            
            Plotly.Plots.resize(plotDiv);
            
            setTimeout(() => {
                window.dispatchEvent(new Event('resize'));
                Plotly.Plots.resize(plotDiv);
            }, 100);
            
        }).catch(error => {
            console.error('Error creating plot:', error);
            showStatus('Error creating plot: ' + error.message, 'error');
        });
        
    } catch (error) {
        console.error('Error in plotTemperatureData:', error);
        showStatus('Error plotting data: ' + error.message, 'error');
        clearPlot();
    }
}

// Get formatted date string for display
function getDateString() {
    const startMonth = parseInt(document.getElementById('startMonth').value);
    const startDay = parseInt(document.getElementById('startDay').value);
    const endMonth = parseInt(document.getElementById('endMonth').value);
    const endDay = parseInt(document.getElementById('endDay').value);
    const monthNames = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ];
    
    if (startMonth === endMonth && startDay === endDay) {
        return `${monthNames[startMonth - 1]} ${startDay}`;
    } else if (startMonth === endMonth) {
        return `${monthNames[startMonth - 1]} ${startDay}-${endDay}`;
    } else {
        return `${monthNames[startMonth - 1]} ${startDay} - ${monthNames[endMonth - 1]} ${endDay}`;
    }
}

// Clear the plot area
function clearPlot() {
    const plotElement = document.getElementById('temperaturePlot');
    if (plotElement) {
        Plotly.purge(plotElement);
        plotElement.innerHTML = '';
        plotElement.style.display = 'flex';
        plotElement.style.alignItems = 'center';
        plotElement.style.justifyContent = 'center';
    }
}

// Refresh cache statistics
async function refreshCacheStats() {
    try {
        const stats = await eel.get_cache_stats()();
        displayCacheStats(stats);
    } catch (error) {
        console.error('Error fetching cache stats:', error);
        document.getElementById('cacheStats').textContent = 'Error loading cache statistics';
    }
}

// Display cache statistics
function displayCacheStats(stats) {
    const statsElement = document.getElementById('cacheStats');
    
    if (!stats) {
        statsElement.textContent = 'No cache statistics available';
        return;
    }
    
    const statsText = `
Total Weather Records: ${stats.total_weather_records || 0}
Locations Cached: ${stats.total_locations_cached || 0}
Unique ZIP Codes: ${stats.unique_zip_codes || 0}
Year Range: ${stats.year_range?.min_year || 'N/A'} - ${stats.year_range?.max_year || 'N/A'}
Database: ${stats.database_path || 'N/A'}
    `.trim();
    
    statsElement.textContent = statsText;
}

// Clear cache
async function clearCache() {
    if (!confirm('Are you sure you want to clear all cached data? This cannot be undone.')) {
        return;
    }
    
    try {
        const result = await eel.clear_cache()();
        if (result.success) {
            showStatus('Cache cleared successfully', 'success');
            refreshCacheStats();
        } else {
            showStatus(`Error clearing cache: ${result.error}`, 'error');
        }
    } catch (error) {
        console.error('Error clearing cache:', error);
        showStatus('Failed to clear cache', 'error');
    }
}

// Export data functionality
function exportData() {
    if (!currentData) {
        showStatus('No data available to export', 'error');
        return;
    }
    
    // Convert data to CSV
    const headers = Object.keys(currentData[0]);
    const csvContent = [
        headers.join(','),
        ...currentData.map(row => headers.map(header => {
            const value = row[header];
            return typeof value === 'string' ? `"${value}"` : value;
        }).join(','))
    ].join('\n');
    
    // Create download link
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `weather_data_${getDateString()}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
    
    showStatus('Data exported successfully', 'success');
}