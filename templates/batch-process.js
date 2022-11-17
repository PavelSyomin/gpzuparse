status_updater = async function() {
    messages = document.querySelector("#messages");
    links = document.querySelector("#links");
    let url = "/batch/tasks/" + task_id;
    let response = await fetch(url);
    if (response.ok) {
        let data = await response.json();
        count = data["data"]["count"];
        total = data["data"]["total"];
        progress.text.textContent = count + "/" + total;
        progress.animate(count / total);

        current_file = data["data"]["current"];
        current_file_info = document.createElement("p");
        current_file_info.textContent = current_file;
        messages.innerHTML = current_file_info.outerHTML;

        if (data["data"]["status"] == "Completed") {
            json_link = document.createElement("a");
            json_link.setAttribute("href", "/batch/tasks/" + task_id + "/json");
            json_link.setAttribute("class", "pure-button pure-button-primary");
            json_link.textContent = "Скачать json";
            links.append(json_link);

            xlsx_link = document.createElement("a");
            xlsx_link.setAttribute("href", "/batch/tasks/" + task_id + "/xlsx");
            xlsx_link.setAttribute("class", "pure-button pure-button-primary");
            xlsx_link.textContent = "Скачать xlsx";
            links.append(xlsx_link);

            log_link = document.createElement("a");
            log_link.setAttribute("href", "/batch/tasks/" + task_id + "/log");
            log_link.setAttribute("class", "pure-button pure-button-primary");
            log_link.textContent = "Скачать лог-файл";
            links.append(log_link);

            return true;
        }
    }
    setTimeout(status_updater, 2000);
}

window.onload = function() {setTimeout(status_updater, 2000, 1000)};
