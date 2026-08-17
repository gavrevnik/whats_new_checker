const state = { items: [], interests: [], trash: [], view: "backlog", mode: "llm", query: "", apiItems: [], llmItems: [], llmErrors: [], tmdb: {}, llm: {}, sortKey: "release_date", sortDirection: "desc", currentDetailId: "", currentPersonId: "", pendingMovieDetails: null, pendingPersonDetails: null, pendingMovieRaw: null, pendingPersonRaw: null, pendingMovieSearchField: "" };
const $ = selector => document.querySelector(selector);
const escapeHtml = (value = "") => String(value).replace(/[&<>'"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[c]));

async function request(url, options = {}) {
  const response = await fetch(url, { headers: {"Content-Type":"application/json"}, ...options });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Не удалось выполнить запрос");
  return payload;
}

function toast(message) {
  const node = $("#toast"); node.textContent = message; node.classList.add("visible");
  clearTimeout(toast.timer); toast.timer = setTimeout(() => node.classList.remove("visible"), 2600);
}

function awardsSummary(raw) {
  try {
    const awards = typeof raw === "string" ? JSON.parse(raw || "[]") : (raw || []);
    if (Array.isArray(awards)) return awards.map(item => item.summary || [item.event, item.award, item.result].filter(Boolean).join(" · ")).filter(Boolean).join("; ") || "—";
    return awards.summary || "—";
  } catch { return String(raw || "—"); }
}

function text(value) { return value === null || value === undefined || value === "" ? "—" : String(value); }
function compactObject(value) { return Object.fromEntries(Object.entries(value).filter(([,item]) => item !== "" && item !== null && item !== undefined)); }
function formatRaw(value) {
  try {
    const parsed = typeof value === "string" ? JSON.parse(value || "{}") : (value || {});
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return text(parsed);
    const labels = {title_original:"Оригинальное название",title_ru:"Название на русском",year:"Год",directors:"Режиссёр",notes:"Заметка",tmdb_id:"TMDB ID",name_original:"Оригинальное имя",name_ru:"Имя на русском",role:"Тип"};
    return Object.entries(parsed).filter(([,item]) => item !== "").map(([key,item]) => `${labels[key] || key}: ${item}`).join(" · ") || "—";
  } catch { return text(value); }
}
function title(item) { return item.title_ru || item.title_original; }
function validUrl(value) { return /^https?:\/\//.test(String(value || "")) ? String(value) : ""; }
function movieLinks(item, includeTmdb = false) {
  const links = [];
  const imdb = validUrl(item.imdb_link) || (item.imdb_id ? `https://www.imdb.com/title/${encodeURIComponent(item.imdb_id)}/` : "");
  const kinopoisk = validUrl(item.kinopoisk_link) || (item.kinopoisk_id ? `https://www.kinopoisk.ru/film/${encodeURIComponent(item.kinopoisk_id)}/` : "");
  const tmdb = validUrl(item.tmdb_link) || (item.tmdb_id ? `https://www.themoviedb.org/movie/${encodeURIComponent(item.tmdb_id)}` : "");
  if (imdb) links.push(["IMDb", imdb]);
  if (kinopoisk) links.push(["КП", kinopoisk]);
  if (includeTmdb && tmdb) links.push(["TMDB", tmdb]);
  return links;
}
function movieLinksHtml(item, includeTmdb = false) {
  const links = movieLinks(item, includeTmdb);
  return links.length
    ? links.map(([label, url]) => `<a class="external-link" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${label} ↗</a>`).join(" ")
    : "—";
}
function movieDraftLinksHtml(item) {
  const links = [];
  const tmdb = validUrl(item?.tmdb_link) || (item?.tmdb_id ? `https://www.themoviedb.org/movie/${encodeURIComponent(item.tmdb_id)}` : "");
  const kinopoisk = validUrl(item?.kinopoisk_link) || (item?.kinopoisk_id ? `https://www.kinopoisk.ru/film/${encodeURIComponent(item.kinopoisk_id)}/` : "");
  if (tmdb) links.push(["TMDB", tmdb]);
  if (kinopoisk) links.push(["КП", kinopoisk]);
  return links.length
    ? links.map(([label, url]) => `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${label} ↗</a>`).join(" ")
    : "—";
}
function renderMovieDraftPreview(item = null) {
  const node = $("#add-movie-preview");
  if (!item) {
    node.innerHTML = ""; node.classList.add("hidden"); return;
  }
  const fields = [
    ["IMDb rating", item.imdb_rating],
    ["Кинопоиск rating", item.kinopoisk_rating],
    ["Актёры", item.cast, true],
    ["Ключевые персоны", formatKeyPeople(item), true],
    ["Описание", item.overview, true],
  ];
  node.innerHTML = fields.map(([label, value, wide]) => `<div class="detail ${wide ? "wide" : ""}"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  node.classList.remove("hidden");
}
function filtered(items) {
  const query = state.query.trim().toLocaleLowerCase("ru");
  if (!query) return items;
  return items.filter(item => [item.title_ru,item.title_original,item.directors,item.genres,item.key_people].join(" ").toLocaleLowerCase("ru").includes(query));
}

function peopleList(value) { return String(value || "").split(";").map(item => item.trim()).filter(Boolean); }
function formatKeyPeople(item, compact = false) {
  const groups = [["Актёры", peopleList(item.key_actors)], ["Режиссёры", peopleList(item.key_directors)]];
  if (compact) {
    const populated = groups.filter(([,people]) => people.length);
    if (!populated.length) return item.key_people || "";
    const total = populated.reduce((sum, [,people]) => sum + people.length, 0);
    return `${populated[0][0]}: ${populated[0][1][0]}${total > 1 ? " и др." : ""}`;
  }
  const parts = groups.filter(([,people]) => people.length).map(([label, people]) => {
    return `${label}: ${people.join("; ")}`;
  });
  return parts.join(" · ") || item.key_people || "";
}

function sortedBacklog(items) {
  const numeric = new Set(["imdb_rating", "kinopoisk_rating"]);
  return [...items].sort((left, right) => {
    const a = left[state.sortKey] ?? "", b = right[state.sortKey] ?? "";
    let result;
    if (numeric.has(state.sortKey)) result = (Number(a) || -Infinity) - (Number(b) || -Infinity);
    else result = String(a).localeCompare(String(b), "ru", {numeric:true, sensitivity:"base"});
    return state.sortDirection === "asc" ? result : -result;
  });
}

function updateSortIndicators() {
  document.querySelectorAll("[data-sort]").forEach(button => {
    const active = button.dataset.sort === state.sortKey;
    button.classList.toggle("active", active); button.dataset.direction = active ? state.sortDirection : "";
    button.setAttribute("aria-sort", active ? (state.sortDirection === "asc" ? "ascending" : "descending") : "none");
  });
}

function movieRow(item, context, index = 0) {
  const action = context === "backlog"
    ? `<button class="mini-button like" data-item-action="like" data-id="${escapeHtml(item.id)}">Нравится</button><button class="mini-button dislike" data-item-action="dislike" data-id="${escapeHtml(item.id)}">Не понравилось</button><button class="icon-button trash-button" data-trash-entity="movie" data-id="${escapeHtml(item.id)}" title="В корзину" aria-label="Переместить фильм в корзину">🗑</button>`
    : `<button class="mini-button done" data-rec-action="backlog" data-index="${index}">В бэклог</button><button class="mini-button like" data-rec-action="like" data-index="${index}">Нравится</button><button class="mini-button dislike" data-rec-action="dislike" data-index="${index}">Не понравилось</button>`;
  const titleCell = context === "backlog"
    ? `<button class="movie-title-button" data-details-id="${escapeHtml(item.id)}"><strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span></button>`
    : `<strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span>`;
  const detailsCell = context === "recommend" ? `<td><button class="mini-button" data-rec-details="${index}">Подробнее</button></td>` : "";
  const tmdbCell = context === "recommend" ? `<td>${escapeHtml(text(item.tmdb_id))}</td>` : "";
  return `<tr>
    <td class="movie-title">${titleCell}</td>
    <td>${escapeHtml(text(item.release_date || item.year))}</td><td>${escapeHtml(text(item.directors))}</td>
    <td>${escapeHtml(text(item.imdb_rating))}</td><td>${escapeHtml(text(item.kinopoisk_rating))}</td><td>${escapeHtml(text(item.genres))}</td>
    <td class="cell-people">${escapeHtml(text(formatKeyPeople(item, true)))}</td>${tmdbCell}
    <td class="cell-links">${movieLinksHtml(item)}</td>
    ${detailsCell}<td><div class="table-actions">${action}</div></td>
  </tr>`;
}

function llmMovieRow(item, index) {
  return `<tr>
    <td class="movie-title"><button type="button" class="movie-title-button" data-llm-details="${index}"><strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span></button></td>
    <td>${escapeHtml(text(item.release_date || item.year))}</td><td>${escapeHtml(text(item.directors))}</td>
    <td>${escapeHtml(text(item.imdb_rating))}</td><td>${escapeHtml(text(item.kinopoisk_rating))}</td><td>${escapeHtml(text(item.genres))}</td>
    <td class="cell-people">${escapeHtml(text(formatKeyPeople(item, true)))}</td><td>${escapeHtml(text(item.tmdb_id))}</td>
    <td class="cell-notes">${escapeHtml(text(item.notes))}</td>
    <td class="cell-links">${movieLinksHtml(item)}</td>
    <td><div class="table-actions"><button type="button" class="mini-button done" data-llm-action="backlog" data-index="${index}">В бэклог</button><button type="button" class="mini-button like" data-llm-action="like" data-index="${index}">Нравится</button><button type="button" class="mini-button dislike" data-llm-action="dislike" data-index="${index}">Не нравится</button></div></td>
  </tr>`;
}

function renderLlmRecommendations() {
  $("#backlog-recommend-body").innerHTML = state.llmItems.length
    ? state.llmItems.map(llmMovieRow).join("")
    : `<tr><td colspan="11" class="empty">Подходящих новых фильмов не найдено</td></tr>`;
  const skipped = state.llmErrors.length;
  const summary = [`Уточнено фильмов: ${state.llmItems.length}.`];
  if (skipped) {
    const examples = state.llmErrors.slice(0, 3).map(item => `${item.title}: ${item.error}`).join("; ");
    summary.push(`Пропущено: ${skipped}.${examples ? ` ${examples}${skipped > 3 ? "; …" : ""}` : ""}`);
  }
  $("#backlog-recommend-message").textContent = summary.join(" ");
  $("#backlog-recommend-message").classList.toggle("error", !state.llmItems.length && skipped > 0);
}

function ratedCard(item) {
  const reaction = item.reaction || "";
  return `<article class="rated-card" data-card-id="${escapeHtml(item.id)}" tabindex="0">
    <h3>${escapeHtml(title(item))}</h3><span class="rated-original">${escapeHtml(item.title_original)}</span><p>${escapeHtml(text(item.directors))} · ${escapeHtml(text(item.year))}</p>
    <div class="rated-actions">
      <button class="mini-button like" data-reaction-toggle="like" data-id="${escapeHtml(item.id)}">${reaction === "like" ? "Убрать лайк" : "Нравится"}</button>
      <button class="mini-button dislike" data-reaction-toggle="dislike" data-id="${escapeHtml(item.id)}">${reaction === "dislike" ? "Убрать дизлайк" : "Не понравилось"}</button>
      <button class="mini-button" data-item-action="backlog" data-id="${escapeHtml(item.id)}">В бэклог</button>
    </div>
  </article>`;
}

function renderLibrary() {
  const backlog = sortedBacklog(filtered(state.items.filter(item => item.status === "backlog")));
  $("#backlog-body").innerHTML = backlog.map(item => movieRow(item, "backlog")).join("");
  $("#backlog-count").textContent = backlog.length; $("#backlog-empty").classList.toggle("hidden", backlog.length > 0); updateSortIndicators();
  const consumed = filtered(state.items.filter(item => item.status === "consumed"));
  const groups = { unrated: consumed.filter(x => !x.reaction), liked: consumed.filter(x => x.reaction === "like"), disliked: consumed.filter(x => x.reaction === "dislike") };
  for (const [key, items] of Object.entries(groups)) {
    $(`#${key}-list`).innerHTML = items.length ? items.map(ratedCard).join("") : `<div class="empty">Пусто</div>`;
    $(`#${key}-count`).textContent = items.length;
  }
}

function showDetails(item) {
  if (!item) return;
  state.currentDetailId = state.items.some(existing => existing.id === item.id) ? item.id : "";
  $("#refresh-detail").classList.toggle("hidden", !state.currentDetailId);
  $("#details-title").textContent = `${title(item)}${item.year ? ` (${item.year})` : ""}`;
  const fields = [
    ["Оригинальное название", item.title_original], ["Дата выхода", item.release_date || item.year], ["Режиссёр", item.directors], ["Жанры", item.genres],
    ["Длительность", item.duration_minutes ? `${item.duration_minutes} мин.` : ""], ["IMDb rating", item.imdb_rating], ["Кинопоиск rating", item.kinopoisk_rating], ["Голоса IMDb", item.imdb_votes], ["Metascore", item.metascore],
    ["TMDB rating", item.tmdb_rating ? `${item.tmdb_rating} (${text(item.tmdb_vote_count)} голосов)` : ""], ["TMDB ID", item.tmdb_id], ["Кинопоиск ID", item.kinopoisk_id], ["Статус", item.movie_status], ["Страны", item.countries],
    ["Сценаристы", item.writers], ["Актёры", item.cast, true], ["Ключевые персоны", formatKeyPeople(item), true],
    ["Награды", awardsSummary(item.awards_json), true], ["Ключевые слова", item.keywords, true], ["Описание", item.overview, true], ["Заметка", item.notes, true], ["Raw · исходный ввод", formatRaw(item.raw_json), true]
  ];
  const cards = fields.map(([label,value,wide]) => `<div class="detail ${wide ? "wide" : ""}"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  const links = `<div class="detail wide"><span>Ссылки</span><p class="detail-links">${movieLinksHtml(item, true)}${validUrl(item.homepage) ? ` <a href="${escapeHtml(item.homepage)}" target="_blank" rel="noreferrer">Сайт ↗</a>` : ""}</p></div>`;
  $("#details-content").innerHTML = cards + links;
  if (!$("#details-dialog").open) $("#details-dialog").showModal();
}

async function patchItem(id, changes) {
  const {item} = await request(`/api/library/${encodeURIComponent(id)}`, {method:"PATCH", body:JSON.stringify(changes)});
  state.items = state.items.map(existing => existing.id === item.id ? item : existing); renderLibrary(); toast("Фильмотека обновлена");
}

function renderPeople() {
  const query = state.query.trim().toLocaleLowerCase("ru");
  const people = state.interests.filter(item => !query || [item.name_ru,item.name_original,item.tmdb_id].join(" ").toLocaleLowerCase("ru").includes(query));
  for (const role of ["director", "actor"]) {
    const rows = people.filter(item => item.role === role);
    $(`#${role}s-body`).innerHTML = rows.length ? rows.map(item => `<tr class="person-row" data-person-details-id="${escapeHtml(item.id)}" tabindex="0" aria-label="Открыть карточку: ${escapeHtml(titlePerson(item))}"><td><strong>${escapeHtml(text(item.name_ru))}</strong></td><td>${escapeHtml(text(item.name_original))}</td><td>${escapeHtml(text(item.tmdb_id))}</td><td><button class="icon-button trash-button" data-trash-entity="person" data-id="${escapeHtml(item.id)}" data-role="${role}" title="В корзину" aria-label="Переместить персону в корзину">🗑</button></td></tr>`).join("") : `<tr><td colspan="4" class="empty">Ничего не найдено</td></tr>`;
    $(`#${role}s-count`).textContent = rows.length;
  }
  $("#people-count").textContent = people.length;
}

function showPersonDetails(person) {
  if (!person) return;
  state.currentPersonId = person.id;
  $("#person-details-title").textContent = titlePerson(person);
  $("#person-details-role").textContent = person.role === "director" ? "Карточка режиссёра" : "Карточка актёра";
  const fields = [
    ["Имя на русском", person.name_ru],
    ["Оригинальное имя", person.name_original],
    ["TMDB ID", person.tmdb_id],
    ["Raw · исходный ввод", formatRaw(person.raw_json)],
  ];
  $("#person-details-content").innerHTML = fields.map(([label, value]) => `<div class="detail"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  if (!$("#person-details-dialog").open) $("#person-details-dialog").showModal();
}

function titlePerson(person) { return person.name_ru || person.name_original || "Персона"; }

function renderTrash() {
  const movies = state.trash.filter(item => item.entity_type === "movie");
  const people = state.trash.filter(item => item.entity_type === "person");
  const restore = item => `<button class="mini-button" data-restore-id="${escapeHtml(item.id)}">Восстановить</button>`;
  $("#trash-movies-body").innerHTML = movies.length ? movies.map(item => `<tr><td><strong>${escapeHtml(text(item.title_ru))}</strong></td><td>${escapeHtml(text(item.title_original))}</td><td>${escapeHtml(text(item.year))}</td><td>${restore(item)}</td></tr>`).join("") : `<tr><td colspan="4" class="empty">Корзина фильмов пуста</td></tr>`;
  $("#trash-people-body").innerHTML = people.length ? people.map(item => `<tr><td><strong>${escapeHtml(text(item.name_ru))}</strong></td><td>${escapeHtml(text(item.name_original))}</td><td>${item.role === "director" ? "Режиссёр" : "Актёр"}</td><td>${restore(item)}</td></tr>`).join("") : `<tr><td colspan="4" class="empty">Корзина персон пуста</td></tr>`;
  $("#trash-movies-count").textContent = movies.length; $("#trash-people-count").textContent = people.length; $("#trash-count").textContent = state.trash.length;
}

function switchView(view) {
  state.view = view; document.querySelectorAll("[data-view]").forEach(button => button.classList.toggle("active", button.dataset.view === view));
  $("#library-view").classList.toggle("hidden", !["backlog", "consumed"].includes(view)); $("#recommend-view").classList.toggle("hidden", view !== "recommend");
  $("#people-view").classList.toggle("hidden", view !== "people"); $("#backlog-panel").classList.toggle("hidden", view !== "backlog"); $("#consumed-panel").classList.toggle("hidden", view !== "consumed");
  $("#trash-view").classList.toggle("hidden", view !== "trash");
  $(".content-choice").classList.toggle("hidden", ["people", "trash"].includes(view)); $("#open-add").classList.toggle("hidden", !["backlog", "consumed"].includes(view)); $("#open-add-person").classList.toggle("hidden", view !== "people");
  $("#search").closest(".search").classList.toggle("hidden", view === "trash"); $("#search").placeholder = view === "people" ? "Найти персону" : "Найти фильм";
  $("#page-title").textContent = ({backlog:"Бэклог", consumed:"Просмотрено", people:"Персоны", recommend:"Рекомендации", trash:"Корзина"})[view]; renderLibrary(); renderPeople(); renderTrash();
}

function renderInterestOptions() {
  for (const role of ["actor","director"]) {
    const items = state.interests.filter(item => item.role === role);
    $(`#${role}-options`).innerHTML = items.map(item => `<label><input type="checkbox" name="${role}_ids" value="${escapeHtml(item.id)}" checked> ${escapeHtml(item.name_original)}</label>`).join("");
    document.querySelector(`[data-select-all="${role}"]`).checked = true;
  }
  updateSelectedLabels();
}

function updateSelectedLabels() {
  for (const role of ["actor","director"]) {
    const boxes = [...document.querySelectorAll(`input[name="${role}_ids"]`)], selected = boxes.filter(box => box.checked).length;
    $(`#${role}s-selected`).textContent = `· ${selected}/${boxes.length}`; const all = document.querySelector(`[data-select-all="${role}"]`);
    all.checked = selected === boxes.length && boxes.length > 0; all.indeterminate = selected > 0 && selected < boxes.length;
  }
}

function recommendationValues() {
  const data = new FormData($("#recommend-form"));
  const values = { actor_ids:data.getAll("actor_ids"), director_ids:data.getAll("director_ids"), limit:data.get("limit") };
  for (const name of ["min_tmdb_rating", "min_imdb_rating", "min_votes", "min_runtime", "year_from", "excluded_genres"]) {
    if (data.has(name)) values[name] = data.get(name);
  }
  return values;
}

function buildPrompt(values) {
  const actorBoxes = [...document.querySelectorAll('input[name="actor_ids"]')], directorBoxes = [...document.querySelectorAll('input[name="director_ids"]')];
  const names = boxes => boxes.filter(box => box.checked).map(box => box.closest("label").textContent.trim()); const actors = names(actorBoxes), directors = names(directorBoxes);
  const actorRule = actors.length === actorBoxes.length ? "все активные актёры из SQLite" : actors.join(", ") || "не использовать актёров как фильтр";
  const directorRule = directors.length === directorBoxes.length ? "все активные режиссёры из SQLite" : directors.join(", ") || "не использовать режиссёров как фильтр";
  const rules = ["фильмы уже вышли"];
  if (values.year_from !== undefined) rules.push(`год выпуска >= ${values.year_from}`);
  if (values.min_tmdb_rating !== undefined) rules.push(`TMDB rating >= ${values.min_tmdb_rating}`);
  if (values.min_imdb_rating !== undefined) rules.push(`IMDb rating >= ${values.min_imdb_rating}`);
  if (values.min_votes !== undefined) rules.push(`минимум ${values.min_votes} голосов TMDB`);
  if (values.min_runtime !== undefined) rules.push(`длительность >= ${values.min_runtime} минут`);
  if (values.excluded_genres !== undefined && values.excluded_genres.trim()) rules.push(`исключить жанры: ${values.excluded_genres}`);
  rules.push(`актёры: ${actorRule}`, `режиссёры: ${directorRule}`, `количество: ${values.limit}`);
  return `Используй $enrich-content-backlog и SQLite-базу data/library.sqlite3. Порекомендуй новые фильмы, исключив весь текущий бэклог, просмотренные фильмы, алиасы и дубли по TMDB ID/названию.\n\nФильтры:\n${rules.map(rule => `- ${rule}`).join("\n")}\n\nВерни таблицу: русское и оригинальное название, дата выхода, режиссёр, IMDb и Кинопоиск rating, жанры, ключевые персоны и ссылки IMDb/КП. Не изменяй SQLite без отдельной команды.`;
}

async function runRecommendation(event) {
  event.preventDefault(); const values = recommendationValues(); if (state.mode === "llm") { $("#prompt-output").value = buildPrompt(values); return; }
  const button = $("#recommend-action"); button.disabled = true; $("#api-message").classList.remove("error"); $("#api-message").textContent = "Получаю данные и исключаю фильмы, уже находящиеся в SQLite…"; $("#recommend-body").innerHTML = "";
  try {
    const payload = {...values};
    if (values.year_from !== undefined) payload.date_from = `${values.year_from}-01-01`;
    delete payload.year_from;
    if (values.excluded_genres !== undefined) payload.excluded_genres = String(values.excluded_genres).split(",").map(x=>x.trim()).filter(Boolean);
    const {items} = await request("/api/recommendations/tmdb", {method:"POST",body:JSON.stringify(payload)}); state.apiItems = items;
    $("#api-message").textContent = items.length ? `Найдено новых фильмов: ${items.length}` : "Новых фильмов по выбранным условиям нет."; $("#recommend-body").innerHTML = items.map((item,index) => movieRow(item,"recommend",index)).join("");
  } catch (error) { $("#api-message").textContent = error.message; $("#api-message").classList.add("error"); } finally { button.disabled = false; }
}

async function addRecommendation(index, action, button) {
  button.disabled = true; const payload = {...state.apiItems[index]}; if (action === "backlog") { payload.status = "backlog"; payload.reaction = ""; } else { payload.status = "consumed"; payload.reaction = action; }
  try {
    const {item} = await request("/api/library", {method:"POST",body:JSON.stringify(payload)}); state.items.push(item); state.apiItems.splice(index,1);
    $("#recommend-body").innerHTML = state.apiItems.map((movie,i)=>movieRow(movie,"recommend",i)).join(""); toast(action === "backlog" ? "Добавлено в бэклог" : "Добавлено в просмотренное");
  } catch (error) { button.disabled = false; toast(error.message); }
}

async function addLlmRecommendation(index, action, button) {
  const source = state.llmItems[index];
  if (!source) return;
  button.disabled = true;
  const payload = {...source};
  if (action === "backlog") { payload.status = "backlog"; payload.reaction = ""; }
  else { payload.status = "consumed"; payload.reaction = action; }
  try {
    const {item} = await request("/api/library", {method:"POST", body:JSON.stringify(payload)});
    state.items.push(item); state.llmItems.splice(index, 1); renderLlmRecommendations(); renderLibrary();
    toast(action === "backlog" ? "Добавлено в бэклог" : "Добавлено в просмотренное");
  } catch (error) { button.disabled = false; toast(error.message); }
}

async function refreshTmdb() {
  const button = $("#refresh-tmdb"), notice = $("#refresh-progress"); button.disabled = true; notice.classList.remove("hidden","error"); notice.textContent = "Актуализирую фильмы и расширенные карточки…";
  try { const result = await request("/api/library/refresh-tmdb", {method:"POST",body:"{}"}); state.items = (await request("/api/library?content_type=movie")).items; renderLibrary(); notice.textContent = `Обновлено ${result.updated} из ${result.total}. Ошибок: ${result.failed}.`; if (result.failed) notice.classList.add("error"); }
  catch (error) { notice.textContent = error.message; notice.classList.add("error"); } finally { button.disabled = false; }
}

async function refreshPeople() {
  const button = $("#refresh-people"), notice = $("#people-refresh-progress"); button.disabled = true; notice.classList.remove("hidden","error"); notice.textContent = "Сверяю имена и TMDB ID…";
  try { const result = await request("/api/people/refresh-tmdb", {method:"POST",body:"{}"}); state.interests = (await request("/api/people")).items; renderPeople(); renderInterestOptions(); notice.textContent = `Обновлено ${result.updated} из ${result.total}. Ошибок: ${result.failed}.`; if (result.failed) notice.classList.add("error"); }
  catch (error) { notice.textContent = error.message; notice.classList.add("error"); } finally { button.disabled = false; }
}

async function refreshCurrentMovie() {
  if (!state.currentDetailId) return;
  const button = $("#refresh-detail"); button.disabled = true; button.textContent = "Обновляю…";
  try {
    const {item} = await request(`/api/library/${encodeURIComponent(state.currentDetailId)}/refresh-tmdb`, {method:"POST",body:"{}"});
    state.items = state.items.map(existing => existing.id === item.id ? item : existing); renderLibrary(); showDetails(item); toast("Карточка обновлена из TMDB");
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = "↻ Актуализировать из TMDB"; }
}

async function refreshCurrentPerson() {
  if (!state.currentPersonId) return;
  const button = $("#refresh-person-detail"); button.disabled = true; button.textContent = "Обновляю…";
  try {
    const {item} = await request(`/api/people/${encodeURIComponent(state.currentPersonId)}/refresh-tmdb`, {method:"POST",body:"{}"});
    state.interests = state.interests.map(existing => existing.id === item.id ? item : existing);
    renderPeople(); renderInterestOptions(); showPersonDetails(item); toast("Персона обновлена из TMDB");
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = "↻ Актуализировать из TMDB"; }
}

function formPayload(form) { return Object.fromEntries(new FormData(form).entries()); }
function fillForm(form, values, fields) {
  for (const field of fields) {
    const control = form.elements.namedItem(field);
    if (control && values[field] !== undefined && values[field] !== null) control.value = values[field];
  }
}
function resolveStatus(selector, message = "", isError = false) {
  const node = $(selector); node.textContent = message; node.classList.toggle("hidden", !message); node.classList.toggle("error", isError);
}
function resetMovieDraft() {
  $("#add-form").reset();
  state.pendingMovieDetails = null; state.pendingMovieRaw = null; state.pendingMovieSearchField = "";
  $("#add-movie-links").textContent = "—"; renderMovieDraftPreview();
  resolveStatus("#add-resolve-status"); $("#add-error").textContent = "";
}
async function saveMovieDraft(reaction = "") {
  const form = $("#add-form"), current = formPayload(form);
  const status = reaction ? "consumed" : "backlog";
  const payload = {
    ...(state.pendingMovieDetails || {}), ...current,
    raw_data: state.pendingMovieRaw || compactObject(current), content_type:"movie", status, reaction,
  };
  const buttons = form.querySelectorAll("[data-save-movie]");
  buttons.forEach(button => { button.disabled = true; });
  try {
    const {item} = await request("/api/library", {method:"POST", body:JSON.stringify(payload)});
    state.items.push(item); resetMovieDraft(); $("#add-dialog").close(); renderLibrary();
    toast(reaction === "like" ? "Добавлено в понравившееся" : reaction === "dislike" ? "Добавлено в не понравившееся" : "Фильм добавлен в бэклог");
  } catch (error) {
    $("#add-error").textContent = error.message;
  } finally {
    buttons.forEach(button => { button.disabled = false; });
  }
}

async function resolveMovieDraft() {
  const form = $("#add-form"), button = $("#resolve-movie"), draft = formPayload(form);
  if (state.pendingMovieSearchField) draft.search_field = state.pendingMovieSearchField;
  state.pendingMovieRaw ??= compactObject(draft); button.disabled = true; button.textContent = "Ищу…"; $("#add-error").textContent = "";
  try {
    const {item} = await request("/api/resolve/movie", {method:"POST", body:JSON.stringify(draft)});
    state.pendingMovieDetails = item; fillForm(form, item, ["title_original", "title_ru", "year", "directors"]);
    state.pendingMovieSearchField = ""; $("#add-movie-links").innerHTML = movieDraftLinksHtml(item); renderMovieDraftPreview(item);
    resolveStatus("#add-resolve-status", `Найдено: ${item.title_ru || item.title_original}${item.year ? ` (${item.year})` : ""}. Исходный ввод сохранится в Raw.`);
  } catch (error) { resolveStatus("#add-resolve-status", error.message, true); }
  finally { button.disabled = false; button.textContent = "↻ Актуализировать из TMDB"; }
}

async function resolvePersonDraft() {
  const form = $("#add-person-form"), button = $("#resolve-person"), draft = formPayload(form);
  state.pendingPersonRaw ??= compactObject(draft); button.disabled = true; button.textContent = "Ищу…"; $("#add-person-error").textContent = "";
  try {
    const {item} = await request("/api/resolve/person", {method:"POST", body:JSON.stringify(draft)});
    state.pendingPersonDetails = item; fillForm(form, item, ["role", "tmdb_id", "name_original", "name_ru"]);
    resolveStatus("#add-person-resolve-status", `Найдено: ${item.name_ru || item.name_original}. Исходный ввод сохранится в Raw.`);
  } catch (error) { resolveStatus("#add-person-resolve-status", error.message, true); }
  finally { button.disabled = false; button.textContent = "↻ Актуализировать из TMDB"; }
}

async function reloadActiveData() {
  const [library, people, trash] = await Promise.all([request("/api/library?content_type=movie"), request("/api/people"), request("/api/trash")]);
  state.items = library.items; state.interests = people.items; state.trash = trash.items;
  renderLibrary(); renderPeople(); renderInterestOptions(); renderTrash();
}

async function moveToTrash(button) {
  button.disabled = true;
  try {
    await request("/api/trash", {method:"POST", body:JSON.stringify({entity_type:button.dataset.trashEntity, entity_id:button.dataset.id, role:button.dataset.role || ""})});
    await reloadActiveData(); toast("Перемещено в корзину");
  } catch (error) { button.disabled = false; toast(error.message); }
}

async function restoreFromTrash(button) {
  button.disabled = true;
  try {
    await request(`/api/trash/${encodeURIComponent(button.dataset.restoreId)}/restore`, {method:"POST", body:"{}"});
    await reloadActiveData(); toast("Восстановлено из корзины");
  } catch (error) { button.disabled = false; toast(error.message); }
}

function openBacklogRecommendation() {
  const currentYear = new Date().getFullYear(), yearTo = $("#backlog-year-to");
  yearTo.max = String(currentYear + 2); yearTo.value = String(currentYear);
  state.llmItems = []; state.llmErrors = [];
  $("#backlog-recommend-error").textContent = ""; $("#backlog-recommend-result").classList.add("hidden"); $("#backlog-recommend-body").innerHTML = ""; $("#backlog-recommend-message").textContent = "";
  const provider = state.llm.provider || "Codex SDK", model = state.llm.model ? ` · ${state.llm.model}` : "";
  $("#backlog-recommend-provider").textContent = `${provider}${model}${state.llm.configured === false ? " · требуется установка" : ""}`;
  $("#backlog-recommend-dialog").showModal();
}

async function runBacklogRecommendation(event) {
  event.preventDefault();
  const button = $("#backlog-recommend-submit"), errorNode = $("#backlog-recommend-error"), resultNode = $("#backlog-recommend-result");
  button.disabled = true; button.textContent = "Подбираю и уточняю…"; errorNode.textContent = ""; resultNode.classList.add("hidden");
  try {
    const payload = formPayload(event.currentTarget);
    const result = await request("/api/recommendations/llm", {method:"POST", body:JSON.stringify(payload)});
    state.llmItems = result.items || []; state.llmErrors = result.errors || []; renderLlmRecommendations();
    $("#backlog-recommend-model").textContent = result.model || "Codex"; resultNode.classList.remove("hidden");
  } catch (error) { errorNode.textContent = error.message; }
  finally { button.disabled = false; button.textContent = "Порекомендовать"; }
}

document.addEventListener("click", async event => {
  const nav = event.target.closest("[data-view]"); if (nav) { switchView(nav.dataset.view); return; }
  const close = event.target.closest("[data-close-dialog]"); if (close) { $(`#${close.dataset.closeDialog}`).close(); return; }
  const sort = event.target.closest("[data-sort]"); if (sort) { const key=sort.dataset.sort; state.sortDirection = state.sortKey === key && state.sortDirection === "asc" ? "desc" : "asc"; state.sortKey=key; renderLibrary(); return; }
  const trash = event.target.closest("[data-trash-entity]"); if (trash) { await moveToTrash(trash); return; }
  const restore = event.target.closest("[data-restore-id]"); if (restore) { await restoreFromTrash(restore); return; }
  const details = event.target.closest("[data-details-id]"); if (details) { showDetails(state.items.find(item=>item.id===details.dataset.detailsId)); return; }
  const personDetails = event.target.closest("[data-person-details-id]"); if (personDetails) { showPersonDetails(state.interests.find(item=>item.id===personDetails.dataset.personDetailsId)); return; }
  const recDetails = event.target.closest("[data-rec-details]"); if (recDetails) { showDetails(state.apiItems[Number(recDetails.dataset.recDetails)]); return; }
  const llmDetails = event.target.closest("[data-llm-details]"); if (llmDetails) { showDetails(state.llmItems[Number(llmDetails.dataset.llmDetails)]); return; }
  const itemAction = event.target.closest("[data-item-action]"); if (itemAction) { const action=itemAction.dataset.itemAction; await patchItem(itemAction.dataset.id,action === "backlog" ? {status:"backlog"} : {status:"consumed",reaction:action}); return; }
  const reaction = event.target.closest("[data-reaction-toggle]"); if (reaction) { const item=state.items.find(x=>x.id===reaction.dataset.id); const next=item.reaction===reaction.dataset.reactionToggle ? "" : reaction.dataset.reactionToggle; await patchItem(item.id,{reaction:next}); return; }
  const rec = event.target.closest("[data-rec-action]"); if (rec) { await addRecommendation(Number(rec.dataset.index),rec.dataset.recAction,rec); return; }
  const llmAction = event.target.closest("[data-llm-action]"); if (llmAction) { await addLlmRecommendation(Number(llmAction.dataset.index),llmAction.dataset.llmAction,llmAction); return; }
  const card = event.target.closest("[data-card-id]"); if (card) { showDetails(state.items.find(item=>item.id===card.dataset.cardId)); return; }
  const mode = event.target.closest("[data-mode]"); if (mode) { state.mode=mode.dataset.mode; document.querySelectorAll("[data-mode]").forEach(x=>x.classList.toggle("active",x===mode)); $("#llm-result").classList.toggle("hidden",state.mode!=="llm"); $("#api-result").classList.toggle("hidden",state.mode!=="api"); $("#recommend-action").textContent=state.mode==="api"?"Обновить рекомендации":"Подготовить промпт"; }
  const filterToggle = event.target.closest("[data-filter-toggle]"); if (filterToggle) { const field=filterToggle.closest("[data-filter-field]"); const input=field.querySelector("input"); input.disabled=!input.disabled; field.classList.toggle("filter-disabled",input.disabled); filterToggle.setAttribute("aria-pressed",String(!input.disabled)); filterToggle.textContent=input.disabled?"—":"✓"; filterToggle.setAttribute("aria-label",`${input.disabled?"Включить":"Отключить"} фильтр ${field.dataset.filterLabel}`); }
});

document.addEventListener("keydown", event => {
  if (event.key !== "Enter" && event.key !== " ") return;
  if (event.target.closest("button, a, input, select, textarea")) return;
  const card=event.target.closest("[data-card-id]"); if (card) { event.preventDefault(); showDetails(state.items.find(item=>item.id===card.dataset.cardId)); return; }
  const person=event.target.closest("[data-person-details-id]"); if (person) { event.preventDefault(); showPersonDetails(state.interests.find(item=>item.id===person.dataset.personDetailsId)); }
});
document.addEventListener("change", event => { const all=event.target.closest("[data-select-all]"); if (all) document.querySelectorAll(`input[name="${all.dataset.selectAll}_ids"]`).forEach(box=>box.checked=all.checked); if (all || event.target.matches('input[name="actor_ids"],input[name="director_ids"]')) updateSelectedLabels(); });
document.querySelectorAll("dialog").forEach(dialog => dialog.addEventListener("click", event => { const rect=dialog.getBoundingClientRect(); if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) dialog.close(); }));

$("#search").addEventListener("input", event => { state.query=event.target.value; renderLibrary(); renderPeople(); }); $("#refresh-tmdb").addEventListener("click", refreshTmdb); $("#refresh-people").addEventListener("click", refreshPeople); $("#refresh-detail").addEventListener("click", refreshCurrentMovie); $("#refresh-person-detail").addEventListener("click", refreshCurrentPerson);
$("#recommend-form").addEventListener("submit", runRecommendation); $("#copy-prompt").addEventListener("click", async()=>{ if(!$("#prompt-output").value)return; await navigator.clipboard.writeText($("#prompt-output").value); toast("Промпт скопирован"); });
$("#open-backlog-recommend").addEventListener("click", openBacklogRecommendation); $("#backlog-recommend-form").addEventListener("submit", runBacklogRecommendation);
$("#open-add").addEventListener("click",()=>{ resetMovieDraft(); $("#add-dialog").showModal(); $("#add-form").elements.namedItem("title_ru").focus(); });
$("#open-add-person").addEventListener("click",()=>{ state.pendingPersonDetails=null;state.pendingPersonRaw=null;$("#add-person-form").reset();resolveStatus("#add-person-resolve-status");$("#add-person-error").textContent="";$("#add-person-dialog").showModal(); });
$("#resolve-movie").addEventListener("click", resolveMovieDraft); $("#resolve-person").addEventListener("click", resolvePersonDraft);
$("#clear-add-movie").addEventListener("click", event => {
  event.preventDefault(); event.stopPropagation(); resetMovieDraft();
  if (!$("#add-dialog").open) $("#add-dialog").showModal();
  $("#add-form").elements.namedItem("title_ru").focus();
});
document.querySelectorAll('[data-save-movie="like"],[data-save-movie="dislike"]').forEach(button => button.addEventListener("click", () => saveMovieDraft(button.dataset.saveMovie)));
$("#add-form").addEventListener("input", event => {
  const field = event.target.name;
  if (!state.pendingMovieDetails || !["title_original", "title_ru", "year"].includes(field)) return;
  state.pendingMovieDetails = null; state.pendingMovieSearchField = field;
  $("#add-movie-links").textContent = "—"; renderMovieDraftPreview();
  resolveStatus("#add-resolve-status", "Данные изменены. Нажмите «Актуализировать из TMDB», чтобы выполнить новый поиск.");
});
$("#add-form").addEventListener("submit", event => { event.preventDefault(); saveMovieDraft(); });
$("#add-person-form").addEventListener("submit",async event=>{ event.preventDefault(); const form=event.currentTarget,current=formPayload(form); const payload={...(state.pendingPersonDetails||{}),...current,raw_data:state.pendingPersonRaw||compactObject(current)}; try{ const {item}=await request("/api/people",{method:"POST",body:JSON.stringify(payload)});state.interests.push(item);form.reset();state.pendingPersonDetails=null;state.pendingPersonRaw=null;$("#add-person-error").textContent="";$("#add-person-dialog").close();renderPeople();renderInterestOptions();toast("Персона добавлена");}catch(error){$("#add-person-error").textContent=error.message;} });

async function initialize() {
  try { const [{items},meta,trash]=await Promise.all([request("/api/library?content_type=movie"),request("/api/meta"),request("/api/trash")]); state.items=items;state.interests=meta.interests;state.trash=trash.items;state.tmdb=meta.tmdb;state.llm=meta.llm||{}; $("#api-status").textContent=meta.tmdb.configured?`TMDB · подключён${meta.tmdb.omdb_configured?" · OMDb":""}${meta.tmdb.kinopoisk_configured?" · КП":""}`:"TMDB · нужен ключ"; renderInterestOptions();renderLibrary();renderPeople();renderTrash(); }
  catch(error){toast(error.message);}
}
initialize();
