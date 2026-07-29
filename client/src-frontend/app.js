const { invoke } = window.__TAURI__.core;

const output = document.getElementById("output");
const breadcrumb = document.getElementById("breadcrumb");
const entryList = document.getElementById("entry-list");
const upBtn = document.getElementById("up-btn");
const uploadBtn = document.getElementById("upload-btn");
const testBtn = document.getElementById("test-btn");

const transferManager = document.getElementById("transfer-manager");
const transferColumn = document.getElementById("transfer-column");
const transferHeader = transferManager.querySelector(".transfer-header");

const username = sessionStorage.getItem("username");
const password = sessionStorage.getItem("password");

// Guard: if someone lands here without logging in (e.g. typed the URL,
// or refreshed after a restart that cleared sessionStorage), bounce back.
if (!username || !password) {
    window.location.href = "index.html";
}

// The full tree, fetched once. Navigation below just walks this in memory.
let rootFolder = null;

// Stack of folders from root to current, e.g. [root, sub, subsub].
// The last entry is always "where we are now".
let pathStack = [];

function showError(err) {
    output.textContent = "Error: " + (err?.toString?.() ?? String(err));
}

function clearError() {
    output.textContent = "";
}

function formatDate(iso) {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    });
}

function currentFolder() {
    return pathStack[pathStack.length - 1];
}

function currentFolderUuid() {
   return currentFolder().uuid;
}

function goToDepth(depth) {
    pathStack = pathStack.slice(0, depth + 1);
    render();
}

function openFolder(folder) {
    pathStack.push(folder);
    render();
}

function goUp() {
    if (pathStack.length > 1) {
        pathStack.pop();
        render();
    }
}

async function upload() {
    const transferId = crypto.randomUUID();
    addTransferRow(transferId);

    try {
        // if your Rust command reports progress via events, listen and call:
        // updateTransferProgress(transferId, percent);
        await invoke("upload", { username, password, folderUuid: currentFolderUuid() });
        updateTransferProgress(transferId, 100);
    } catch (err) {
        showError(err);
    } finally {
        removeTransferRow(transferId);
        fetchMap();
    }
}

async function test() {
  try {
    await invoke("test_dialog");
    fetchMap();
  }
  catch (err) {
      showError(err);
  }
}

function renderBreadcrumb() {
    breadcrumb.innerHTML = "";
    pathStack.forEach((folder, i) => {
        const crumb = document.createElement("span");
        crumb.className = "crumb";
        crumb.textContent = folder.name;
        if (i < pathStack.length - 1) {
            crumb.classList.add("crumb-link");
            crumb.addEventListener("click", () => goToDepth(i));
        } else {
            crumb.classList.add("crumb-current");
        }
        breadcrumb.appendChild(crumb);

        if (i < pathStack.length - 1) {
            const sep = document.createElement("span");
            sep.className = "crumb-sep";
            sep.textContent = "/";
            breadcrumb.appendChild(sep);
        }
    });

    upBtn.disabled = pathStack.length <= 1;
}

function buildFolderRow(folder) {
    const row = document.createElement("div");
    row.className = "entry-card folder-row";

    const title = document.createElement("div");
    title.className = "entry-title";
    title.textContent = `${folder.name}`;
    row.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "entry-meta";
    meta.textContent = `Updated ${formatDate(folder.last_changed_at)}`;
    row.appendChild(meta);

    row.addEventListener("click", () => openFolder(folder));
    return row;
}

function buildFileRow(file) {
    const row = document.createElement("div");
    row.className = "entry-card file-row";

    const title = document.createElement("div");
    title.className = "entry-title";
    title.textContent = `${file.name}`;
    row.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "entry-meta";
    meta.textContent = `Updated ${formatDate(file.last_changed_at)}`;
  row.appendChild(meta);

  const delete_btn = document.createElement("button");
  delete_btn.className = "delete-btn";
  delete_btn.textContent = `Delete`;
  delete_btn.addEventListener("click", () => deleteFile(file.uuid))

  row.appendChild(delete_btn);

    return row;
}

function render() {
    const folder = currentFolder();
    renderBreadcrumb();
    entryList.innerHTML = "";

    const hasFolders = folder.folders && folder.folders.length > 0;
    const hasFiles = folder.files && folder.files.length > 0;

    if (!hasFolders && !hasFiles) {
        entryList.innerHTML = '<p class="empty">This folder is empty.</p>';
        return;
    }

    if (hasFolders) {
        folder.folders.forEach((f) => entryList.appendChild(buildFolderRow(f)));
    }
    if (hasFiles) {
        folder.files.forEach((f) => entryList.appendChild(buildFileRow(f)));
    }
}

async function fetchMap() {
    clearError();
    try {
      rootFolder = await invoke("request_map", { username, password });
      console.log(rootFolder);
        pathStack = [rootFolder];
        render();
    } catch (err) {
        showError(err);
    }
}

upBtn.addEventListener("click", goUp);
uploadBtn.addEventListener("click", upload);
testBtn.addEventListener("click", test);

document.getElementById("logout-btn").addEventListener("click", () => {
    sessionStorage.removeItem("username");
    sessionStorage.removeItem("password");
    window.location.href = "index.html";
});

document.getElementById("refresh-btn").addEventListener("click", fetchMap);

document.getElementById("run-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("run_task");
        output.textContent =
            typeof result === "string" ? result : JSON.stringify(result, null, 2);
    } catch (err) {
        showError(err);
    }
});

document.getElementById("greet-btn").addEventListener("click", async () => {
    try {
        const result = await invoke("greet", { name: "World" });
        output.textContent = result;
    } catch (err) {
        showError(err);
    }
});

function updateTransferHeader() {
    const count = transferColumn.children.length;
    transferHeader.textContent = `Transferring ${count} file${count === 1 ? "" : "s"}`;
    transferManager.style.display = count === 0 ? "none" : "";
}

function addTransferRow(id) {
    const row = document.createElement("div");
    row.className = "transfer-row";
    row.dataset.transferId = id;

    const item = document.createElement("div");
    item.className = "transfer-item";

    const nameSpan = document.createElement("span");
    nameSpan.className = "file-name";
    nameSpan.innerHTML = `
        <svg class="file-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
            <path d="M14 2v6h6"/>
        </svg>
        ${id}
    `;
    item.appendChild(nameSpan);

    const percentSpan = document.createElement("span");
    percentSpan.className = "file-percent";
    percentSpan.textContent = "0%";
    item.appendChild(percentSpan);

    row.appendChild(item);

    const track = document.createElement("div");
    track.className = "transfer-progress-track";
    const bar = document.createElement("div");
    bar.className = "transfer-progress-bar";
    bar.style.width = "0%";
    track.appendChild(bar);
    row.appendChild(track);

    transferColumn.appendChild(row);
    updateTransferHeader();
    return row;
}

async function deleteFile(uuid) {
  try {
    await invoke("delete_file", { username, password, uuid });
    fetchMap();
  }
  catch (err) {
    showError(err)
  }
}

function updateTransferProgress(id, percent) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (!row) return;
    row.querySelector(".file-percent").textContent = `${percent}%`;
    row.querySelector(".transfer-progress-bar").style.width = `${percent}%`;
}

function removeTransferRow(id) {
    const row = transferColumn.querySelector(`.transfer-row[data-transfer-id="${id}"]`);
    if (row) row.remove();
    updateTransferHeader();
}


// Run automatically as soon as the page loads.
fetchMap();
