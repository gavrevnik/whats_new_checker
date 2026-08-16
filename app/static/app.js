const state = { items: [], interests: [], view: "backlog", mode: "llm", query: "", apiItems: [], tmdb: {}, sortKey: "release_date", sortDirection: "desc", currentDetailId: "" };
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
function title(item) { return item.title_ru || item.title_original; }
function validUrl(value) { return /^https?:\/\//.test(String(value || "")) ? String(value) : ""; }
function externalLink(item) {
  return validUrl(item.imdb_link) || (item.imdb_id ? `https://www.imdb.com/title/${encodeURIComponent(item.imdb_id)}/` : "") ||
    validUrl(item.tmdb_link) || (item.tmdb_id ? `https://www.themoviedb.org/movie/${encodeURIComponent(item.tmdb_id)}` : "") || validUrl(item.url);
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
  const numeric = new Set(["imdb_rating", "tmdb_id"]);
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
    ? `<button class="mini-button done" data-item-action="consume" data-id="${escapeHtml(item.id)}">Просмотрено</button>`
    : `<button class="mini-button done" data-rec-action="backlog" data-index="${index}">В бэклог</button><button class="mini-button like" data-rec-action="like" data-index="${index}">Нравится</button><button class="mini-button dislike" data-rec-action="dislike" data-index="${index}">Не понравилось</button>`;
  const link = externalLink(item);
  const titleCell = context === "backlog"
    ? `<button class="movie-title-button" data-details-id="${escapeHtml(item.id)}"><strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span></button>`
    : `<strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span>`;
  const detailsCell = context === "recommend" ? `<td><button class="mini-button" data-rec-details="${index}">Подробнее</button></td>` : "";
  return `<tr>
    <td class="movie-title">${titleCell}</td>
    <td>${escapeHtml(text(item.release_date || item.year))}</td><td>${escapeHtml(text(item.directors))}</td>
    <td>${escapeHtml(text(item.imdb_rating))}</td><td>${escapeHtml(text(item.genres))}</td>
    <td class="cell-people">${escapeHtml(text(formatKeyPeople(item, true)))}</td><td>${escapeHtml(text(item.tmdb_id))}</td>
    <td>${link ? `<a class="external-link" href="${escapeHtml(link)}" target="_blank" rel="noreferrer">${item.imdb_id ? "IMDb ↗" : "TMDB ↗"}</a>` : "—"}</td>
    ${detailsCell}<td><div class="table-actions">${action}</div></td>
  </tr>`;
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
  const link = externalLink(item);
  const fields = [
    ["Оригинальное название", item.title_original], ["Дата выхода", item.release_date || item.year], ["Режиссёр", item.directors], ["Жанры", item.genres],
    ["Длительность", item.duration_minutes ? `${item.duration_minutes} мин.` : ""], ["IMDb rating", item.imdb_rating], ["Голоса IMDb", item.imdb_votes], ["Metascore", item.metascore],
    ["TMDB rating", item.tmdb_rating ? `${item.tmdb_rating} (${text(item.tmdb_vote_count)} голосов)` : ""], ["Статус", item.movie_status], ["Страны", item.countries],
    ["Сценаристы", item.writers], ["Актёры", item.cast, true], ["Ключевые персоны", formatKeyPeople(item), true],
    ["Награды", awardsSummary(item.awards_json), true], ["Ключевые слова", item.keywords, true], ["Описание", item.overview, true], ["Заметка", item.notes, true]
  ];
  const cards = fields.map(([label,value,wide]) => `<div class="detail ${wide ? "wide" : ""}"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  const links = `<div class="detail wide"><span>Ссылки</span><p class="detail-links">${link ? `<a href="${escapeHtml(link)}" target="_blank" rel="noreferrer">${item.imdb_id ? "IMDb" : "TMDB"} ↗</a>` : "—"}${item.tmdb_id && item.imdb_id ? ` <a href="https://www.themoviedb.org/movie/${escapeHtml(item.tmdb_id)}" target="_blank" rel="noreferrer">TMDB ↗</a>` : ""}${validUrl(item.homepage) ? ` <a href="${escapeHtml(item.homepage)}" target="_blank" rel="noreferrer">Сайт ↗</a>` : ""}</p></div>`;
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
    $(`#${role}s-body`).innerHTML = rows.length ? rows.map(item => `<tr><td>${escapeHtml(text(item.name_ru))}</td><td><strong>${escapeHtml(text(item.name_original))}</strong></td><td>${escapeHtml(text(item.tmdb_id))}</td></tr>`).join("") : `<tr><td colspan="3" class="empty">Ничего не найдено</td></tr>`;
    $(`#${role}s-count`).textContent = rows.length;
  }
  $("#people-count").textContent = people.length;
}

function switchView(view) {
  state.view = view; document.querySelectorAll("[data-view]").forEach(button => button.classList.toggle("active", button.dataset.view === view));
  $("#library-view").classList.toggle("hidden", !["backlog", "consumed"].includes(view)); $("#recommend-view").classList.toggle("hidden", view !== "recommend");
  $("#people-view").classList.toggle("hidden", view !== "people"); $("#backlog-panel").classList.toggle("hidden", view !== "backlog"); $("#consumed-panel").classList.toggle("hidden", view !== "consumed");
  $(".content-choice").classList.toggle("hidden", view === "people"); $("#open-add").classList.toggle("hidden", view === "people"); $("#open-add-person").classList.toggle("hidden", view !== "people");
  $("#search").placeholder = view === "people" ? "Найти персону" : "Найти фильм";
  $("#page-title").textContent = ({backlog:"Бэклог", consumed:"Просмотрено", people:"Персоны", recommend:"Рекомендации"})[view]; renderLibrary(); renderPeople();
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
  return { actor_ids:data.getAll("actor_ids"), director_ids:data.getAll("director_ids"), min_tmdb_rating:data.get("min_tmdb_rating"), min_imdb_rating:data.get("min_imdb_rating"), min_votes:data.get("min_votes"), min_runtime:data.get("min_runtime"), year_from:data.get("year_from"), limit:data.get("limit"), excluded_genres:data.get("excluded_genres") };
}

function buildPrompt(values) {
  const actorBoxes = [...document.querySelectorAll('input[name="actor_ids"]')], directorBoxes = [...document.querySelectorAll('input[name="director_ids"]')];
  const names = boxes => boxes.filter(box => box.checked).map(box => box.closest("label").textContent.trim()); const actors = names(actorBoxes), directors = names(directorBoxes);
  const actorRule = actors.length === actorBoxes.length ? "все активные актёры из SQLite" : actors.join(", ") || "не использовать актёров как фильтр";
  const directorRule = directors.length === directorBoxes.length ? "все активные режиссёры из SQLite" : directors.join(", ") || "не использовать режиссёров как фильтр";
  return `Используй $enrich-content-backlog и SQLite-базу data/library.sqlite3. Порекомендуй новые фильмы, исключив весь текущий бэклог, просмотренные фильмы, алиасы и дубли по TMDB ID/названию.\n\nФильтры:\n- фильмы уже вышли\n- год выпуска >= ${values.year_from}\n- TMDB rating >= ${values.min_tmdb_rating}\n- IMDb rating >= ${values.min_imdb_rating}\n- минимум ${values.min_votes} голосов TMDB\n- длительность >= ${values.min_runtime} минут\n- исключить жанры: ${values.excluded_genres || "нет"}\n- актёры: ${actorRule}\n- режиссёры: ${directorRule}\n- количество: ${values.limit}\n\nВерни таблицу: русское и оригинальное название, дата выхода, режиссёр, IMDb rating, жанры, ключевые персоны, TMDB ID и ссылки на IMDb/TMDB. Не изменяй SQLite без отдельной команды.`;
}

async function runRecommendation(event) {
  event.preventDefault(); const values = recommendationValues(); if (state.mode === "llm") { $("#prompt-output").value = buildPrompt(values); return; }
  const button = $("#recommend-action"); button.disabled = true; $("#api-message").classList.remove("error"); $("#api-message").textContent = "Получаю данные и исключаю фильмы, уже находящиеся в SQLite…"; $("#recommend-body").innerHTML = "";
  try {
    const payload = {...values, date_from:`${values.year_from}-01-01`, excluded_genres:String(values.excluded_genres||"").split(",").map(x=>x.trim()).filter(Boolean)};
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

document.addEventListener("click", async event => {
  const nav = event.target.closest("[data-view]"); if (nav) { switchView(nav.dataset.view); return; }
  const close = event.target.closest("[data-close-dialog]"); if (close) { $(`#${close.dataset.closeDialog}`).close(); return; }
  const sort = event.target.closest("[data-sort]"); if (sort) { const key=sort.dataset.sort; state.sortDirection = state.sortKey === key && state.sortDirection === "asc" ? "desc" : "asc"; state.sortKey=key; renderLibrary(); return; }
  const details = event.target.closest("[data-details-id]"); if (details) { showDetails(state.items.find(item=>item.id===details.dataset.detailsId)); return; }
  const recDetails = event.target.closest("[data-rec-details]"); if (recDetails) { showDetails(state.apiItems[Number(recDetails.dataset.recDetails)]); return; }
  const itemAction = event.target.closest("[data-item-action]"); if (itemAction) { await patchItem(itemAction.dataset.id,{status:itemAction.dataset.itemAction === "consume" ? "consumed" : "backlog"}); return; }
  const reaction = event.target.closest("[data-reaction-toggle]"); if (reaction) { const item=state.items.find(x=>x.id===reaction.dataset.id); const next=item.reaction===reaction.dataset.reactionToggle ? "" : reaction.dataset.reactionToggle; await patchItem(item.id,{reaction:next}); return; }
  const rec = event.target.closest("[data-rec-action]"); if (rec) { await addRecommendation(Number(rec.dataset.index),rec.dataset.recAction,rec); return; }
  const card = event.target.closest("[data-card-id]"); if (card) { showDetails(state.items.find(item=>item.id===card.dataset.cardId)); return; }
  const mode = event.target.closest("[data-mode]"); if (mode) { state.mode=mode.dataset.mode; document.querySelectorAll("[data-mode]").forEach(x=>x.classList.toggle("active",x===mode)); $("#llm-result").classList.toggle("hidden",state.mode!=="llm"); $("#api-result").classList.toggle("hidden",state.mode!=="api"); $("#recommend-action").textContent=state.mode==="api"?"Обновить рекомендации":"Подготовить промпт"; }
});

document.addEventListener("keydown", event => { const card=event.target.closest("[data-card-id]"); if (card && (event.key === "Enter" || event.key === " ")) { event.preventDefault(); showDetails(state.items.find(item=>item.id===card.dataset.cardId)); } });
document.addEventListener("change", event => { const all=event.target.closest("[data-select-all]"); if (all) document.querySelectorAll(`input[name="${all.dataset.selectAll}_ids"]`).forEach(box=>box.checked=all.checked); if (all || event.target.matches('input[name="actor_ids"],input[name="director_ids"]')) updateSelectedLabels(); });
document.querySelectorAll("dialog").forEach(dialog => dialog.addEventListener("click", event => { const rect=dialog.getBoundingClientRect(); if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) dialog.close(); }));

$("#search").addEventListener("input", event => { state.query=event.target.value; renderLibrary(); renderPeople(); }); $("#refresh-tmdb").addEventListener("click", refreshTmdb); $("#refresh-people").addEventListener("click", refreshPeople); $("#refresh-detail").addEventListener("click", refreshCurrentMovie);
$("#recommend-form").addEventListener("submit", runRecommendation); $("#copy-prompt").addEventListener("click", async()=>{ if(!$("#prompt-output").value)return; await navigator.clipboard.writeText($("#prompt-output").value); toast("Промпт скопирован"); });
$("#open-add").addEventListener("click",()=>$("#add-dialog").showModal()); $("#open-add-person").addEventListener("click",()=>$("#add-person-dialog").showModal());
$("#add-form").addEventListener("submit",async event=>{ event.preventDefault(); const form=event.currentTarget; const payload={...Object.fromEntries(new FormData(form).entries()),content_type:"movie",status:"backlog"}; try{ const {item}=await request("/api/library",{method:"POST",body:JSON.stringify(payload)});state.items.push(item);form.reset();$("#add-error").textContent="";$("#add-dialog").close();renderLibrary();toast("Фильм добавлен");}catch(error){$("#add-error").textContent=error.message;} });
$("#add-person-form").addEventListener("submit",async event=>{ event.preventDefault(); const form=event.currentTarget; const payload=Object.fromEntries(new FormData(form).entries()); try{ const {item}=await request("/api/people",{method:"POST",body:JSON.stringify(payload)});state.interests.push(item);form.reset();$("#add-person-error").textContent="";$("#add-person-dialog").close();renderPeople();renderInterestOptions();toast("Персона добавлена");}catch(error){$("#add-person-error").textContent=error.message;} });

async function initialize() {
  try { const [{items},meta]=await Promise.all([request("/api/library?content_type=movie"),request("/api/meta")]); state.items=items;state.interests=meta.interests;state.tmdb=meta.tmdb; $("#api-status").textContent=meta.tmdb.configured?`TMDB · подключён${meta.tmdb.omdb_configured?" · OMDb":""}`:"TMDB · нужен ключ"; renderInterestOptions();renderLibrary();renderPeople(); }
  catch(error){toast(error.message);}
}
initialize();
