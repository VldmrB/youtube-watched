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
let buttonsToDisableWhenWorkingDB = [takeoutSubmitButton, updateRecordsButton, newProjectButton, takeoutCancelButton];

let visualizeButton = document.querySelector("#visualize-button");

visualizeButton.onclick = function() {
    window.location = "/dash/";
};


function disableOrEnableSomeButtons() {
    for (let i = 0; i < buttonsToDisableWhenWorkingDB.length; i++) {
        // noinspection RedundantIfStatementJS
        if (buttonsToDisableWhenWorkingDB[i].disabled) {
            buttonsToDisableWhenWorkingDB[i].disabled = false;
        } else {
            buttonsToDisableWhenWorkingDB[i].disabled = true;
        }
    }
}

function wipeProgressIndicators(preserveMsg = false) {
    document.querySelector("#progress-bar-container").style.visibility = "hidden";
    progressBar.style.width = "0%";
    progressBarPercentage.innerHTML = "0.0%";
    if (!preserveMsg) {
        progressMsg.innerHTML = "";
        progressMsg.style.color = "black";
    }
}

function closeEventSource() {
    progress.close();
    console.log('Closed event source')
}

function makeCancelButtonCancel() {
    takeoutCancelButton.setAttribute("disabled", "true");
    progressUnfinishedDBProcessWarning.innerHTML = "Stopping the process, please wait...";
    let cancelAJAX = new XMLHttpRequest();
    cancelAJAX.open("POST", "cancel_db_process");
    cancelAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    function resetTakeoutProcess() {
        if (cancelAJAX.readyState === 4 && cancelAJAX.status === 200) {
            progressUnfinishedDBProcessWarning.innerHTML = "";
            takeoutCancelButton.removeAttribute("disabled");
            disableOrEnableSomeButtons();
            if (progressMsg.innerHTML.indexOf("watch-history") !== -1 || progressMsg.innerHTML === "") {
                // process interrupted before watch-history file(s) parsing was finished, no progress stats to show
                wipeProgressIndicators();
            }
        }
    }
    cancelAJAX.addEventListener("readystatechange", resetTakeoutProcess);
    cancelAJAX.send();
}

function retrieveActiveProcess () {
    function activeProcessResults () {
        if (this.readyState === 4 && this.status === 200) {
            let response = JSON.parse(this.responseText);
            if (response["stage"] !== "Quiet") {
                disableOrEnableSomeButtons();
                takeoutCancelButton.disabled = false;
                document.querySelector("#progress-bar-container").style.visibility = "visible";
                takeoutCancelButton.style.visibility = "visible";
                if (progressMsg.innerHTML === "") {
                    progressMsg.innerHTML = response["stage"];
                    if (progressBarPercentage.innerHTML === ""){
                        progressBar.style.width = response["percent"] + "%";
                        progressBarPercentage.innerHTML = response["percent"] + "%";
                    }
                }

            }
        }
    }
    let AJAX = new XMLHttpRequest();
    AJAX.open("GET", "/process_status");
    AJAX.addEventListener("readystatechange", activeProcessResults);
    AJAX.send();
}


// ------------------------ Event Source listeners for various event types {Start} ------------------------
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
    wipeProgressIndicators(true);
    // enable the Visualize button since the DB now has some records
    if (visualizeButton.disabled) {visualizeButton.removeAttribute("disabled");}
};

let onEventError = function(event) {
    if (event.data !== undefined) {
        progressMsg.innerHTML = event.data;
        progressMsg.style.color = "red";
        disableOrEnableSomeButtons();
        wipeProgressIndicators(true);
    }
};

let onEventMsg = function (event) {
    let progressVal = event.data + "%";
    progressBar.style.width = progressVal;
    progressBarPercentage.innerHTML = progressVal;
};
// ------------------------ Event Source listeners for various event types {End} ------------------------


let progress = new EventSource("/db_progress_stream");

progress.addEventListener("stage", onEventStage);
progress.addEventListener("stats", onEventStats);
progress.addEventListener("errors", onEventError);
progress.addEventListener("message", onEventMsg);

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
        }
    }
}


window.addEventListener("beforeunload", closeEventSource);
takeoutCancelButton.addEventListener("click", makeCancelButtonCancel);

function processTakeout(event) {
    event.preventDefault();

    let idOfElementActedOn = this.id;
    let takeoutDirectoryVal = document.querySelector("#takeout-input").value;

    let anAJAX = new XMLHttpRequest();
    anAJAX.addEventListener("readystatechange", showProgress);
    if (idOfElementActedOn === "takeout-form") {
        anAJAX.open("POST", "/convert_takeout");
        anAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
        anAJAX.send("takeout-dir=" + takeoutDirectoryVal);
    } else {
        anAJAX.open("GET", "/update_records");
        anAJAX.send();
    }

}

retrieveActiveProcess();
takeoutSubmit.addEventListener("submit", processTakeout);
updateRecordsButton.addEventListener("click", processTakeout);