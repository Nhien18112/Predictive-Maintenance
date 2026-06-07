document.addEventListener("DOMContentLoaded", () => {
    // Endpoints mapping
    const endpoints = {
        pdm: {
            grafana: "http://localhost:3000/d/pdm-overview/pdm-gold-overview?kiosk",
            superset: "http://localhost:8088/superset/dashboard/pdm-machine-detail/?standalone=1"
        },
        phm: {
            grafana: "http://localhost:3000/d/pdm-overview_phm/phm-gold-overview?kiosk",
            superset: "http://localhost:8088/superset/dashboard/phm-machine-detail/?standalone=1"
        }
    };

    const textContent = {
        pdm: {
            title: "Predictive Maintenance (PDM)",
            subtitle: "Real-time sensor telemetry and anomaly detection"
        },
        phm: {
            title: "Prognostics & Health Mgmt (PHM)",
            subtitle: "Remaining Useful Life (RUL) predictions and historical trends"
        }
    };

    // State
    let currentTopic = "pdm"; // 'pdm' or 'phm'
    let currentTool = "grafana"; // 'grafana' or 'superset'

    // DOM Elements
    const navBtns = document.querySelectorAll('.nav-btn');
    const subtabBtns = document.querySelectorAll('.subtab-btn');
    const iframe = document.getElementById('dashboard-frame');
    const titleEl = document.getElementById('topic-title');
    const subtitleEl = document.getElementById('topic-subtitle');

    let supersetLoggedIn = false;
    let loginPromise = fetch("http://localhost:8088/api/v1/security/csrf_token/", {
        credentials: "include"
    })
    .then(res => res.json())
    .then(data => {
        let csrf = data.result;
        return fetch("http://localhost:8088/login/", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: `username=admin&password=admin&csrf_token=${csrf}`,
            credentials: "include"
        });
    })
    .then(() => {
        supersetLoggedIn = true;
    }).catch(err => console.error("Superset auto-login failed:", err));

    // Function to update the view
    async function updateDashboard() {
        if (currentTool === 'superset' && !supersetLoggedIn) {
            await loginPromise;
        }
        
        // 1. Update Iframe Source
        const newSrc = endpoints[currentTopic][currentTool];
        // Only reload iframe if source changed to prevent flickering
        if (iframe.src !== newSrc) {
            // Remove iframe momentarily to re-trigger fade-in animation
            iframe.style.animation = 'none';
            iframe.offsetHeight; /* trigger reflow */
            iframe.style.animation = null; 
            iframe.src = newSrc;
        }

        // 2. Update Text content
        titleEl.textContent = textContent[currentTopic].title;
        subtitleEl.textContent = textContent[currentTopic].subtitle;

        // 3. Update Body Theme Class
        if (currentTopic === 'phm') {
            document.body.classList.add('theme-phm');
        } else {
            document.body.classList.remove('theme-phm');
        }
    }

    // Event Listeners for Primary Nav (Topics)
    navBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            navBtns.forEach(b => b.classList.remove('active'));
            const clickedBtn = e.currentTarget;
            clickedBtn.classList.add('active');
            
            currentTopic = clickedBtn.getAttribute('data-topic');
            updateDashboard();
        });
    });

    // Event Listeners for Secondary Tabs (Tools)
    subtabBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            subtabBtns.forEach(b => b.classList.remove('active'));
            const clickedBtn = e.currentTarget;
            clickedBtn.classList.add('active');
            
            currentTool = clickedBtn.getAttribute('data-tool');
            updateDashboard();
        });
    });

    // Initial load
    updateDashboard();
});
