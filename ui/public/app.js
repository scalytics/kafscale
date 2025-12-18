const metricFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 1 });
let topicsCache = [];
let selectedTopicName = null;

async function fetchStatus() {
  const summary = document.getElementById('cluster-summary');
  try {
    const resp = await fetch('/ui/api/status');
    if (!resp.ok) throw new Error('status ' + resp.status);
    const data = await resp.json();
    summary.textContent = `${data.cluster} · version ${data.version} · brokers ${data.brokers.ready}/${data.brokers.desired}`;

    renderHealth(data);

    topicsCache = data.topics || [];
    renderTopics(topicsCache);
  } catch (err) {
    summary.textContent = `Failed to load status: ${err.message}`;
  }
}

function renderHealth(data) {
  const container = document.getElementById('health-cards');
  container.innerHTML = '';
  const cards = [
    { title: 'Brokers', value: `${data.brokers.ready}/${data.brokers.desired}` },
    { title: 'S3 State', value: (data.s3.state || 'unknown').toUpperCase() },
    { title: 'S3 Pressure', value: (data.s3.backpressure || 'unknown').toUpperCase() },
    { title: 'etcd', value: (data.etcd.state || 'unknown').toUpperCase() },
  ];
  cards.forEach(card => {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `<h3>${card.title}</h3><strong>${card.value}</strong>`;
    container.appendChild(div);
  });

  renderBrokers(data.brokers.nodes || []);

  const alerts = document.getElementById('alerts');
  alerts.innerHTML = '';
  if (!data.alerts || data.alerts.length === 0) {
    const ok = document.createElement('p');
    ok.textContent = 'No active alerts';
    alerts.appendChild(ok);
  } else {
    data.alerts.forEach(alert => {
      const div = document.createElement('div');
      div.className = `alert ${alert.level}`;
      div.textContent = alert.message;
      alerts.appendChild(div);
    });
  }
}

function renderBrokers(nodes) {
  const grid = document.getElementById('broker-grid');
  grid.innerHTML = '';
  if (!nodes.length) {
    grid.textContent = 'No broker data';
    return;
  }
  nodes.forEach(node => {
    const card = document.createElement('div');
    card.className = 'broker-card';
    card.innerHTML = `
      <div><span class="status-dot ${node.state}"></span><strong>${node.name}</strong></div>
      <div>State: ${node.state.toUpperCase()}</div>
      <div>Partitions: ${node.partitions}</div>
      <div>CPU: ${node.cpu}% · Mem: ${node.memory}%</div>
      <div>Backpressure: ${node.backpressure.toUpperCase()}</div>
    `;
    grid.appendChild(card);
  });
}

function startMetricsStream() {
  const produceEl = document.getElementById('produce-rps');
  const fetchEl = document.getElementById('fetch-rps');
  const latencyEl = document.getElementById('live-s3-latency');
  const status = document.getElementById('metrics-status');
  const source = new EventSource('/ui/api/metrics');
  source.onmessage = event => {
    try {
      const payload = JSON.parse(event.data);
      const metrics = payload.metrics || {};
      setMetricValue(produceEl, metrics.produce_rps);
      setMetricValue(fetchEl, metrics.fetch_rps);
      setMetricValue(latencyEl, metrics.s3_latency_ms);
      if (payload.timestamp) {
        const ts = new Date(payload.timestamp);
        status.textContent = `Last update ${ts.toLocaleTimeString()}`;
      } else {
        status.textContent = 'Live metrics';
      }
    } catch (err) {
      status.textContent = 'Metrics parse error';
    }
  };
  source.onerror = () => {
    status.textContent = 'Stream disconnected';
    source.close();
  };
}

function setMetricValue(el, value) {
  if (!el) return;
  if (typeof value === 'number' && !Number.isNaN(value)) {
    el.textContent = metricFormatter.format(value);
  }
}

function renderTopics(topics) {
  const tableBody = document.querySelector('#topic-table tbody');
  tableBody.innerHTML = '';
  if (!topics.length) {
    tableBody.innerHTML = '<tr><td colspan="3">No topics</td></tr>';
    renderTopicDetail(null);
    return;
  }
  if (!selectedTopicName || !topics.some(t => t.name === selectedTopicName)) {
    selectedTopicName = topics[0].name;
  }
  topics.forEach(topic => {
    const row = document.createElement('tr');
    row.dataset.topic = topic.name;
    row.innerHTML = `<td>${topic.name}</td><td>${topic.partitions}</td><td>${topic.state}</td>`;
    if (topic.name === selectedTopicName) {
      row.classList.add('selected');
    }
    row.addEventListener('click', () => {
      selectedTopicName = topic.name;
      updateTopicSelection();
      renderTopicDetail(topic);
    });
    tableBody.appendChild(row);
  });
  const selected = topics.find(t => t.name === selectedTopicName) || null;
  renderTopicDetail(selected);
}

function updateTopicSelection() {
  document.querySelectorAll('#topic-table tbody tr').forEach(row => {
    if (row.dataset.topic === selectedTopicName) {
      row.classList.add('selected');
    } else {
      row.classList.remove('selected');
    }
  });
}

function renderTopicDetail(topic) {
  const panel = document.getElementById('topic-detail');
  if (!panel) return;
  if (!topic) {
    panel.innerHTML = '<p>Select a topic to inspect partition layout.</p>';
    return;
  }
  const partitions = topic.partitions_details || [];
  const header = `
    <div class="topic-header">
      <h3>${topic.name}</h3>
      <p>${topic.partitions} partitions · State ${topic.state.toUpperCase()}</p>
    </div>
  `;
  if (!partitions.length) {
    panel.innerHTML = header + '<p>No partition metadata reported.</p>';
    return;
  }
  const rows = partitions.map(part => `
    <tr>
      <td>${part.id}</td>
      <td>${part.leader}</td>
      <td>${part.replicas}</td>
      <td>${part.isr}</td>
    </tr>
  `).join('');
  panel.innerHTML = `
    ${header}
    <table class="detail-table">
      <thead>
        <tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

fetchStatus();
startMetricsStream();
setInterval(fetchStatus, 10000);
