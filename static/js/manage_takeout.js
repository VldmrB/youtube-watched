// this block is responsible for when the project has just been created and there's no Takeout yet
toggleElementVisibility("takeout-setup-button", "takeout-setup", "Cancel", "takeout-input");
let addMoreTakeoutButton = document.querySelector("#takeout-setup-button");
if (!document.querySelector("#takeout-setup").classList.contains("hidden")
    && addMoreTakeoutButton.innerHTML !== "Cancel"
) {
    let curButtonWidth = addMoreTakeoutButton.offsetWidth;
    addMoreTakeoutButton.innerHTML = "Cancel";
    addMoreTakeoutButton.style.width = curButtonWidth + "px";
}

if (!document.querySelector("#takeout-section").dataset.full) {
document.querySelector("#takeout-setup").classList.remove("hidden");
}

document.querySelector("#new-project-button").onclick = function() {
    window.location = "/setup_project";
};

let progressMsg = document.querySelector("#progress-msg");
let progressBar = document.querySelector("#progress-bar");
let progressBarPercentage = document.querySelector("#progress-bar-text span");
let progressUnfinishedDBProcessWarning = document.querySelector("#wait-for-db");
let takeoutSubmit = document.querySelector("#takeout-form");
let takeoutSubmitButton = takeoutSubmit.querySelector("input[type='submit']");
let updateRecordsButton = document.querySelector("#update-records-button");
let takeoutCancelButton = document.querySelector("#takeout-cancel-button");
let newProjectButton = document.querySelector("#new-project-button");
let buttonsToDisableWhenWorkingDB = [takeoutSubmitButton, updateRecordsButton, newProjectButton];

let visualizeButton = document.querySelector("#visualize-button");

visualizeButton.onclick = function() {
    window.location = "/dash/";
};

let progress;

function disableOrEnableSomeButtons() {
    for (let i = 0; i < buttonsToDisableWhenWorkingDB.length; i++) {
        if (!buttonsToDisableWhenWorkingDB[i].getAttribute("disabled")) {
            buttonsToDisableWhenWorkingDB[i].setAttribute("disabled", "true");
        } else {
            buttonsToDisableWhenWorkingDB[i].removeAttribute("disabled");
        }
    }
}

function wipeProgressIndicators(preserveMsg = false) {
    document.querySelector("#progress-bar-container").style.visibility = "hidden";
    takeoutCancelButton.style.visibility = "hidden";
    progressBar.style.width = "0%";
    progressBarPercentage.innerHTML = "0%";
    progressMsg.style.color = "black";
    if (!preserveMsg) {progressMsg.innerHTML = "";}
}

function closeEventSource() {
    try {
        progress.close();
        console.log('Closed event source')
    } catch(err) {
        console.log('Event source already closed')
    }
}

function cleanUpAfterTakeoutInsertion() {
    takeoutCancelButton.style.visibility = "hidden";
    closeEventSource();
}

function closeEventStreamServerSide() {
    let AJAX = new XMLHttpRequest();
    AJAX.open("POST", "stop_event_stream");
    AJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    AJAX.send();
}

function makeCancelButtonCancel() {
    takeoutCancelButton.setAttribute("disabled", "true");
    progressUnfinishedDBProcessWarning.innerHTML = "Stopping the process, please wait...";
    let cancelAJAX = new XMLHttpRequest();
    cancelAJAX.open("POST", "cancel_db_process");
    cancelAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    function cancelTakeout() {
        if (cancelAJAX.readyState === 4 && cancelAJAX.status === 200) {
            progressUnfinishedDBProcessWarning.innerHTML = "";
            cleanUpAfterTakeoutInsertion();
            disableOrEnableSomeButtons();
            if (progressMsg.innerHTML.indexOf("watch-history") !== -1 || progressMsg.innerHTML === "") {
                // process interrupted before watch-history file(s) parsing was finished, no progress stats to show
                closeEventStreamServerSide();
                wipeProgressIndicators();
            }
            takeoutCancelButton.removeAttribute("disabled");
        }
    }
    cancelAJAX.addEventListener("readystatechange", cancelTakeout);
    cancelAJAX.send();
}


// ------------------------ Event Source listeners for various events {Start} ------------------------
let onEventStage = function(event) {progressMsg.innerHTML = event.data;};

let onEventStats = function(event) {
    let msgJSON = JSON.parse(event.data);
    let msgString = "";
    if (msgJSON.hasOwnProperty("inserted")) {
        msgString += "Inserted: " + msgJSON["inserted"] + "<br>";
    }
    if (msgJSON["updated"] !== 0) {
        msgString += "Updated: " + msgJSON["updated"] + "<br>";
    }
    if (msgJSON["failed_api_requests"] !== 0) {
        msgString += ("Failed API requests: " + msgJSON["failed_api_requests"] +
            " (run this again to attempt these 1-2 more times.)" + "<br>");
    }
    msgString += "Total in the database: " + msgJSON["records_in_db"];

    progressMsg.innerHTML = msgString;
    if (addMoreTakeoutButton.getAttribute("disabled")) {
        disableOrEnableSomeButtons()
    }
    cleanUpAfterTakeoutInsertion();
    wipeProgressIndicators(true);
    // enable the Visualize button since the DB now has some records
    if (visualizeButton.disabled === true) {visualizeButton.removeAttribute("disabled");}
};

let onEventError = function(event) {
    progressMsg.style.color = "red";
    progressMsg.innerHTML = event.data;
    cleanUpAfterTakeoutInsertion();
};

let onEventMsg = function (event) {
    let progressVal = event.data + "%";
    progressBar.style.width = progressVal;
    progressBarPercentage.innerHTML = progressVal;
};

function showProgress() {
    if (this.readyState === 4 && this.status === 200) {
        if (this.responseText.indexOf("Wait for") !== -1) {
            if (progressUnfinishedDBProcessWarning.innerHTML === "") {
                progressUnfinishedDBProcessWarning.innerHTML = this.responseText;
                setTimeout(function() {progressUnfinishedDBProcessWarning.innerHTML = "";}, 3000);
            }
        } else {
            wipeProgressIndicators(); // reset the progress bar/messages for a fresh round
            document.querySelector("#progress-bar-container").style.visibility = "visible";
            takeoutCancelButton.style.visibility = "visible";
            disableOrEnableSomeButtons();
            progress.addEventListener("stage", onEventStage);
            progress.addEventListener("stats", onEventStats);
            progress.addEventListener("error", onEventError);
            progress.addEventListener("message", onEventMsg);
        }
    }
}
// ------------------------ Event Source listeners for various events {End} ------------------------


window.addEventListener("beforeunload", closeEventSource);
takeoutCancelButton.addEventListener("click", makeCancelButtonCancel);

function processTakeout(event) {
    event.preventDefault();

    let idOfElementActedOn = this.id;
    let takeoutDirectoryVal = document.querySelector("#takeout-input").value;
    progress = new EventSource("/db_progress_stream");


    let anAJAX = new XMLHttpRequest();
    if (idOfElementActedOn === "takeout-form") {
        anAJAX.open("POST", "/convert_takeout");
        anAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
        anAJAX.addEventListener("readystatechange", showProgress);
        anAJAX.send("takeout-dir=" + takeoutDirectoryVal);
    } else {
        anAJAX.open("GET", "/update_records");
        anAJAX.addEventListener("readystatechange", showProgress);
        anAJAX.send();
    }

}

takeoutSubmit.addEventListener("submit", processTakeout);
updateRecordsButton.addEventListener("click", processTakeout);