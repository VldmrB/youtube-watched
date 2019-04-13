document.querySelector("#new-project-button").onclick = function() {
    window.location = "/setup_project";
};

let dbState = document.querySelector("#takeout-section").dataset.full === 'true';

let progressMsg = document.querySelector("#progress-msg");
let processResults = document.querySelector("#process-results");
let progressBar = document.querySelector("#progress-bar");
let progressBarPercentage = document.querySelector("#progress-bar-text span");
let progressUnfinishedDBProcessWarning = document.querySelector("#wait-for-db");
let takeoutSubmit = document.querySelector("#takeout-form");
let takeoutSubmitButton = takeoutSubmit.querySelector("#takeout-form input[type='submit']");
let updateRecordsButton = document.querySelector("#update-form input[type='submit']");
let takeoutCancelButton = document.querySelector("#takeout-cancel-button");
let newProjectButton = document.querySelector("#new-project-button");
// let buttonsToDisableWhenWorkingDB = [takeoutSubmitButton, updateRecordsButton, newProjectButton];

let visualizeButton = document.querySelector("#visualize-button");

visualizeButton.onclick = function() {
    window.location = "/dash/";
};


function addMsg(data, textColor = 'black', header = false){
    let entry = document.createElement('div');
    entry.style.color = textColor;
    if (header) {
        entry.style.fontWeight = 'bold';
        entry.style.fontStyle = 'italic'
    }
    entry.innerHTML = (data);
    progressMsg.appendChild(entry);
}

function disableOrEnableButtons(disable = true) {
    if (disable) {
        takeoutSubmitButton.disabled = true;
        updateRecordsButton.disabled =  true;
        newProjectButton.disabled =  true;
        takeoutCancelButton.disabled = false;
    } else {
        takeoutCancelButton.disabled = true;
        takeoutSubmitButton.disabled = false;
        newProjectButton.disabled =  false;
        if (dbState) {
            updateRecordsButton.disabled =  false;
        }
    }
}

function wipeProgressIndicators(wipeResults = false) {
    document.querySelector("#progress-bar-container").style.visibility = "hidden";
    progressBar.style.width = "0%";
    progressBarPercentage.innerHTML = "0.0%";
    if (wipeResults) {
        processResults.innerHTML = "";
        processResults.style.color = "black";
    }
}

function closeEventSource() {
    progress.close();
    console.log('Closed event source')
}

function makeCancelButtonCancel() {
    takeoutCancelButton.disabled = true;
    progressUnfinishedDBProcessWarning.innerHTML = "Stopping the process, please wait...";
    let cancelAJAX = new XMLHttpRequest();
    cancelAJAX.open("POST", "cancel_db_process");
    cancelAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    function resetTakeoutProcess() {
        if (cancelAJAX.readyState === 4 && cancelAJAX.status === 200) {
            progressUnfinishedDBProcessWarning.innerHTML = "";
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
                disableOrEnableButtons(true);
                document.querySelector("#progress-bar-container").style.visibility = "visible";
                takeoutCancelButton.style.visibility = "visible";
                if (progressMsg.innerHTML === "") {
                    addMsg(response["stage"]);
                    if (progressBarPercentage.innerHTML === ""){
                        if (response["percent"].includes(' ')) {
                            let progressVal = response["percent"].split(" ");
                            let currentFile = Number(progressVal[0]);
                            let fileAmount =  Number(progressVal[1]);

                            progressBar.style.width = (Number(currentFile / fileAmount).toFixed(2) * 100) + "%";
                            progressBarPercentage.innerHTML = (currentFile+1) + " of " + fileAmount;
                        } else {
                            progressBar.style.width = response["percent"] + "%";
                            progressBarPercentage.innerHTML = response["percent"] + "%";

                        }

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
let onEventStage = function(event) {
    addMsg('<br>' + event.data, 'black', true);
};

let onEventInfo = function(event) {
    addMsg(event.data);
};

let onEventStats = function(event) {
    let msgJSON = JSON.parse(event.data);
    let msgString = "";
    if (msgJSON.hasOwnProperty("inserted") && msgJSON["inserted"] !== 0) {
        msgString += "Inserted video records: " + msgJSON["inserted"] + "<br>";
    }
    if (msgJSON["updated"] !== 0) {
        msgString += "Updated: " + msgJSON["updated"] + "<br>";
    }
    if (msgJSON.hasOwnProperty("newly_inactive") && msgJSON["newly_inactive"] !== 0) {
        msgString += "Videos no longer available through API (likely taken down): " +
            msgJSON["newly_inactive"] + "<br>";
    }
    if (msgJSON.hasOwnProperty("newly_active") && msgJSON["newly_active"] !== 0) {
        msgString += "Videos now available through API (weren't previously): " +
            msgJSON["newly_active"] + "<br>";
    }
    if (msgJSON.hasOwnProperty("deleted") && msgJSON["deleted"] !== 0) {
        msgString += "Videos removed from YouTube: " +
            msgJSON["deleted"] + "<br>";
    }
    if (msgJSON.hasOwnProperty("timestamps") && msgJSON["timestamps"] !== 0) {
        msgString += "Total timestamps: " +
            msgJSON["timestamps"] + "<br>";
    }
    msgString += "Total video records: " + msgJSON["records_in_db"];
    if (msgJSON["records_in_db"] > 0 && dbState === false) {
        dbState = true;
        visualizeButton.disabled = false;
    }
    processResults.innerHTML = msgString;
};

let onEventStop = function() {
    wipeProgressIndicators();
    disableOrEnableButtons(false);
};

let onEventError = function(event) {
    let errorData;
    if (event.data !== undefined) {
        errorData = event.data;
    } else {
        errorData = 'Unspecified error';
    }
    addMsg(errorData, 'red');
    disableOrEnableButtons(false);
    wipeProgressIndicators();
};

let onEventWarning = function(event) {
    addMsg(event.data, 'red');
};

let onEventTakeoutProgress = function (event) {
    let progressVal = event.data.split(" ");
    let currentFile = Number(progressVal[0]);
    let fileAmount =  Number(progressVal[1]);
    progressBar.style.width = (Number(currentFile / fileAmount).toFixed(2) * 100) + "%";
    progressBarPercentage.innerHTML = (currentFile+1) + " of " + fileAmount;
};

let onEventMsg = function (event) {
    let msgData = event.data.split(" ");
    let progressVal = msgData[0] + "%";
    progressBar.style.width = progressVal;
    progressBarPercentage.innerHTML = progressVal + ' / #' + msgData[1];

};
// ------------------------ Event Source listeners for various event types {End} ------------------------


let progress = new EventSource("/db_progress_stream");

progress.addEventListener("info", onEventInfo);
progress.addEventListener("stage", onEventStage);
progress.addEventListener("stats", onEventStats);
progress.addEventListener("stop", onEventStop);
progress.addEventListener("errors", onEventError);
progress.addEventListener('warnings', onEventWarning);
progress.addEventListener("takeout_progress", onEventTakeoutProgress);
progress.addEventListener("message", onEventMsg);

function showProgress() {
    if (this.readyState === 4 && this.status === 200) {
        if (this.responseText.indexOf("Wait for") !== -1) {
            if (progressUnfinishedDBProcessWarning.innerHTML === "") {
                progressUnfinishedDBProcessWarning.innerHTML = this.responseText;
                setTimeout(function() {progressUnfinishedDBProcessWarning.innerHTML = "";}, 3000);
            }
        } else {
            // reset the progress bar/messages for a fresh round
            progressMsg.innerHTML = "";
            wipeProgressIndicators(true);
            document.querySelector("#progress-bar-container").style.visibility = "visible";
            takeoutCancelButton.style.visibility = "visible";
            disableOrEnableButtons(true);
        }
    }
}


window.addEventListener("beforeunload", closeEventSource);
takeoutCancelButton.addEventListener("click", makeCancelButtonCancel);

function processTakeout(event) {
    event.preventDefault();

    let idOfElementActedOn = this.id;

    let logging_verbosity= document.querySelector("#logging-verbosity-level").value;

    let anAJAX = new XMLHttpRequest();
    anAJAX.addEventListener("readystatechange", showProgress);
    anAJAX.open("POST", "/start_db_process");
    anAJAX.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    if (idOfElementActedOn === "takeout-form") {
        let takeoutDirectoryVal = document.querySelector("#takeout-input").value;
        anAJAX.send("takeout-dir=" + takeoutDirectoryVal + "&logging-verbosity-level=" + logging_verbosity);
    } else {
        let updateCutoff = document.querySelector("#update-form input[name='update-cutoff']").value;
        let updateCutoffDenomination = document.querySelector("#update-cutoff-periods").value;
        anAJAX.send("update-cutoff=" + updateCutoff + "&update-cutoff-denomination=" + updateCutoffDenomination +
        "&logging-verbosity-level=" + logging_verbosity);
    }

}

retrieveActiveProcess();
takeoutSubmit.addEventListener("submit", processTakeout);
updateRecordsButton.addEventListener("click", processTakeout);