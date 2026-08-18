const state = { items: [], interests: [], trash: [], view: "backlog", contentType: "movie", backlogRecommendMode: "llm", query: "", apiItems: [], llmItems: [], llmErrors: [], apiProgressWarnings: [], recommendationSort: {key:"year",direction:"desc"}, tmdb: {}, musicbrainz: {}, llm: {}, sortKey: "release_date", sortDirection: "desc", peopleSort: {director:{key:"name_ru",direction:"asc"},actor:{key:"name_ru",direction:"asc"},artist:{key:"name_original",direction:"asc"}}, currentDetailId: "", currentPersonId: "", pendingMovieDetails: null, pendingAlbumDetails: null, pendingPersonDetails: null, pendingArtistDetails: null, pendingMovieRaw: null, pendingAlbumRaw: null, pendingPersonRaw: null, pendingArtistRaw: null, pendingMovieSearchField: "", pendingAlbumSearchField: "" };
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
function movieRatings(item) { return `${text(item.imdb_rating)} / ${text(item.kinopoisk_rating)}`; }
function providerWarningText(warning) {
  const labels = {"cover-art-archive":"Cover Art Archive",listenbrainz:"ListenBrainz",musicbrainz:"MusicBrainz",kinopoisk:"Кинопоиск","tmdb-images":"TMDB Images",tmdb:"TMDB"};
  const label = labels[String(warning?.provider || "").toLowerCase()] || warning?.provider || "API", message = String(warning?.message || "");
  if (!message) return "";
  return message.toLowerCase().includes(String(label).toLowerCase()) ? message : `${label}: ${message}`;
}
function providerWarnings(items) {
  const messages = items.flatMap(item => item?.provider_warnings || []).map(providerWarningText).filter(Boolean);
  return [...new Set(messages)];
}
function showProviderWarnings(items) {
  const warnings = [...new Set([...state.apiProgressWarnings.map(providerWarningText), ...providerWarnings(items)].filter(Boolean))], node = $("#backlog-recommend-warning");
  node.textContent = warnings.join(" "); node.classList.toggle("hidden", !warnings.length);
}
function compactObject(value) { return Object.fromEntries(Object.entries(value).filter(([,item]) => item !== "" && item !== null && item !== undefined)); }
function formatRaw(value) {
  try {
    const parsed = typeof value === "string" ? JSON.parse(value || "{}") : (value || {});
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return text(parsed);
    const labels = {title_original:"Оригинальное название",title_ru:"Название на русском",year:"Год",directors:"Режиссёр",artists:"Исполнитель",notes:"Заметка",tmdb_id:"TMDB ID",release_group_mbid:"MusicBrainz ID",mbid:"MusicBrainz ID",name:"Исполнитель",name_original:"Оригинальное имя",name_ru:"Имя на русском",role:"Тип"};
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
function albumLinksHtml(item) {
  const mbid = item?.release_group_mbid || item?.mbid;
  const url = validUrl(item?.musicbrainz_link) || (mbid ? `https://musicbrainz.org/release-group/${encodeURIComponent(mbid)}` : "");
  return url ? `<a class="external-link" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">MusicBrainz ↗</a>` : "—";
}
function youtubeMusicAlbumUrl(item) {
  const year = item?.year || String(item?.first_release_date || "").slice(0, 4);
  const query = [item?.title_original || title(item), year ? `(${year})` : "", item?.artists].filter(Boolean).join(" ");
  return `https://music.youtube.com/search?${new URLSearchParams({q:query}).toString()}`;
}
function youtubeMusicAlbumLinkHtml(item) {
  return `<a class="youtube-music-link" href="${escapeHtml(youtubeMusicAlbumUrl(item))}" target="_blank" rel="noreferrer">Слушать в YouTube Music ↗</a>`;
}
function listenCount(item) { return item.total_listen_count === null || item.total_listen_count === undefined || item.total_listen_count === "" ? "—" : new Intl.NumberFormat("ru-RU").format(Number(item.total_listen_count)); }
function localArtworkUrl(relativePath) {
  return relativePath ? `/media/artwork/${String(relativePath).split("/").map(encodeURIComponent).join("/")}` : "";
}
function artworkItemUrl(item, contentType) {
  return item?.id && item?.added_at ? `/api/artwork/${contentType}/${encodeURIComponent(item.id)}` : "";
}
function albumCoverHtml(item, className = "album-cover") {
  const available = item?.cover_path || validUrl(item?.cover_url);
  const url = available && (artworkItemUrl(item,"music") || localArtworkUrl(item?.cover_path) || validUrl(item?.cover_url));
  return url ? `<img class="${className}" src="${escapeHtml(url)}" alt="Обложка ${escapeHtml(title(item))}" loading="lazy" data-artwork>` : `<span class="album-cover-placeholder" aria-hidden="true"></span>`;
}
function moviePosterHtml(item, className = "movie-poster") {
  const available = item?.poster_local_path || item?.poster_path || validUrl(item?.poster_url);
  const url = available && (artworkItemUrl(item,"movie") || localArtworkUrl(item?.poster_local_path) || validUrl(item?.poster_url));
  return url ? `<img class="${className}" src="${escapeHtml(url)}" alt="Постер ${escapeHtml(title(item))}" loading="lazy" data-artwork>` : `<span class="movie-poster-placeholder" aria-hidden="true"></span>`;
}
function albumTypes(item) {
  let secondary = item.secondary_types || item.secondary_types_json || [];
  if (typeof secondary === "string") { try { secondary = JSON.parse(secondary); } catch { secondary = []; } }
  return [item.primary_type, ...(Array.isArray(secondary) ? secondary : [])].filter(Boolean).join(" · ");
}
function formatTrackList(value) {
  if (!Array.isArray(value) || !value.length) return "—";
  return value.map(track => `${track.number ? `${track.number}. ` : ""}${track.title || ""}`).join("; ");
}
function renderAlbumDraftPreview(item = null) {
  const node = $("#add-album-preview");
  if (!item) { node.innerHTML = ""; node.classList.add("hidden"); return; }
  const fields = [["Исполнитель",item.artists],["Первый выпуск",item.first_release_date || item.year],["Количество песен",item.track_count],["Тип",albumTypes(item)],["Жанры",item.genres],["Прослушивания ListenBrainz",listenCount(item)]];
  node.innerHTML = fields.map(([label,value]) => `<div class="detail"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  node.classList.remove("hidden");
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
  const poster = item.poster_local_path || item.poster_url ? `<div class="detail wide"><span>Постер</span>${moviePosterHtml(item,"detail-poster")}</div>` : "";
  node.innerHTML = poster + fields.map(([label, value, wide]) => `<div class="detail ${wide ? "wide" : ""}"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  node.classList.remove("hidden");
}
function filtered(items) {
  const query = state.query.trim().toLocaleLowerCase("ru");
  if (!query) return items;
  return items.filter(item => [item.title_ru,item.title_original,item.directors,item.artists,item.genres,item.key_people].join(" ").toLocaleLowerCase("ru").includes(query));
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
  const numeric = new Set(["imdb_rating", "kinopoisk_rating", "total_listen_count", "track_count", "year"]);
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

function movieRow(item) {
  const action = `<button class="mini-button like" data-item-action="like" data-id="${escapeHtml(item.id)}">Нравится</button><button class="mini-button dislike" data-item-action="dislike" data-id="${escapeHtml(item.id)}">Не понравилось</button><button class="icon-button trash-button" data-trash-entity="movie" data-id="${escapeHtml(item.id)}" title="В корзину" aria-label="Переместить фильм в корзину">🗑</button>`;
  const titleCell = `<button class="movie-title-button" data-details-id="${escapeHtml(item.id)}"><strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span></button>`;
  return `<tr>
    <td class="movie-poster-cell">${moviePosterHtml(item)}</td><td class="movie-title">${titleCell}</td>
    <td>${escapeHtml(text(item.release_date || item.year))}</td><td>${escapeHtml(text(item.directors))}</td>
    <td>${escapeHtml(movieRatings(item))}</td><td>${escapeHtml(text(item.countries))}</td><td>${escapeHtml(text(item.genres))}</td>
    <td class="cell-people">${escapeHtml(text(formatKeyPeople(item, true)))}</td>
    <td class="cell-links">${movieLinksHtml(item)}</td>
    <td><div class="table-actions">${action}</div></td>
  </tr>`;
}

function albumRow(item) {
  const action = `<button class="mini-button like" data-item-action="like" data-id="${escapeHtml(item.id)}">Нравится</button><button class="mini-button dislike" data-item-action="dislike" data-id="${escapeHtml(item.id)}">Не понравилось</button><button class="icon-button trash-button" data-trash-entity="album" data-id="${escapeHtml(item.id)}" title="В корзину" aria-label="Переместить альбом в корзину">🗑</button>`;
  const titleCell = `<button class="movie-title-button" data-details-id="${escapeHtml(item.id)}"><strong>${escapeHtml(item.title_original || title(item))}</strong><span>${escapeHtml(text(item.artists))}</span></button>`;
  return `<tr><td class="album-cover-cell">${albumCoverHtml(item)}</td><td class="movie-title">${titleCell}</td><td>${escapeHtml(text(item.artists))}</td><td>${escapeHtml(text(item.first_release_date || item.year))}</td><td>${escapeHtml(text(item.track_count))}</td><td>${escapeHtml(text(albumTypes(item)))}</td><td>${escapeHtml(text(item.genres))}</td><td>${escapeHtml(listenCount(item))}</td><td class="cell-links"><a class="external-link" href="${escapeHtml(youtubeMusicAlbumUrl(item))}" target="_blank" rel="noreferrer">YouTube Music ↗</a></td><td><div class="table-actions">${action}</div></td></tr>`;
}

function recommendationMovieRow(item, index) {
  const notes = state.backlogRecommendMode === "llm" ? `<td class="cell-notes">${escapeHtml(text(item.notes))}</td>` : "";
  return `<tr>
    <td class="movie-poster-cell">${moviePosterHtml(item)}</td><td class="movie-title"><button type="button" class="movie-title-button" data-modal-rec-details="${index}"><strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span></button></td>
    <td>${escapeHtml(text(item.release_date || item.year))}</td><td>${escapeHtml(text(item.directors))}</td>
    <td>${escapeHtml(movieRatings(item))}</td><td>${escapeHtml(text(item.countries))}</td><td>${escapeHtml(text(item.genres))}</td>
    <td class="cell-people">${escapeHtml(text(formatKeyPeople(item, true)))}</td>
    ${notes}
    <td class="cell-links">${movieLinksHtml(item)}</td>
    <td><div class="table-actions"><button type="button" class="mini-button done" data-modal-rec-action="backlog" data-index="${index}">В бэклог</button><button type="button" class="mini-button like" data-modal-rec-action="like" data-index="${index}">Нравится</button><button type="button" class="mini-button dislike" data-modal-rec-action="dislike" data-index="${index}">Не нравится</button><button type="button" class="mini-button remove" data-modal-rec-action="remove" data-index="${index}">Убрать</button></div></td>
  </tr>`;
}

function recommendationAlbumRow(item, index) {
  const notes = state.backlogRecommendMode === "llm" ? `<td class="cell-notes">${escapeHtml(text(item.notes))}</td>` : "";
  return `<tr><td class="album-cover-cell">${albumCoverHtml(item)}</td><td class="movie-title"><button type="button" class="movie-title-button" data-modal-rec-details="${index}"><strong>${escapeHtml(title(item))}</strong><span>${escapeHtml(item.title_original)}</span></button></td><td>${escapeHtml(text(item.artists))}</td><td>${escapeHtml(text(item.first_release_date || item.year))}</td><td>${escapeHtml(text(item.track_count))}</td><td>${escapeHtml(text(albumTypes(item)))}</td><td>${escapeHtml(text(item.genres))}</td><td>${escapeHtml(listenCount(item))}</td>${notes}<td class="cell-links">${albumLinksHtml(item)}</td><td><div class="table-actions"><button type="button" class="mini-button done" data-modal-rec-action="backlog" data-index="${index}">В бэклог</button><button type="button" class="mini-button like" data-modal-rec-action="like" data-index="${index}">Нравится</button><button type="button" class="mini-button dislike" data-modal-rec-action="dislike" data-index="${index}">Не нравится</button><button type="button" class="mini-button remove" data-modal-rec-action="remove" data-index="${index}">Убрать</button></div></td></tr>`;
}

function recommendationSortValue(item, key) {
  if (key === "title") return title(item);
  if (key === "year") return item.year || item.first_release_date || item.release_date || "";
  if (key === "rating") return item.imdb_rating ?? item.tmdb_rating ?? "";
  if (key === "types") return albumTypes(item);
  if (key === "key_people") return formatKeyPeople(item, true);
  return item[key] ?? "";
}

function sortedRecommendationEntries(items) {
  const numeric = new Set(["year","rating","track_count","total_listen_count"]), setting = state.recommendationSort;
  return items.map((item,index) => ({item,index})).sort((left,right) => {
    const a = recommendationSortValue(left.item,setting.key), b = recommendationSortValue(right.item,setting.key);
    let result = numeric.has(setting.key)
      ? (Number(String(a).slice(0,4)) || -Infinity) - (Number(String(b).slice(0,4)) || -Infinity)
      : String(a).localeCompare(String(b),"ru",{numeric:true,sensitivity:"base"});
    if (!result) result = title(left.item).localeCompare(title(right.item),"ru",{numeric:true,sensitivity:"base"});
    return setting.direction === "asc" ? result : -result;
  });
}

function updateRecommendationSortIndicators() {
  document.querySelectorAll("[data-recommend-sort]").forEach(button => {
    const active = button.dataset.recommendSort === state.recommendationSort.key;
    button.classList.toggle("active",active); button.dataset.direction = active ? state.recommendationSort.direction : "";
    button.setAttribute("aria-sort",active ? (state.recommendationSort.direction === "asc" ? "ascending" : "descending") : "none");
  });
}

function renderLlmRecommendations() {
  $("#backlog-recommend-body").innerHTML = state.llmItems.length
    ? sortedRecommendationEntries(state.llmItems).map(({item,index}) => (state.contentType === "music" ? recommendationAlbumRow : recommendationMovieRow)(item,index)).join("")
    : `<tr><td colspan="11" class="empty">Подходящих новых ${state.contentType === "music" ? "альбомов" : "фильмов"} не найдено</td></tr>`;
  const skipped = state.llmErrors.length;
  const summary = [`Уточнено ${state.contentType === "music" ? "альбомов" : "фильмов"}: ${state.llmItems.length}.`];
  if (skipped) {
    const examples = state.llmErrors.slice(0, 3).map(item => `${item.title}: ${item.error}`).join("; ");
    summary.push(`Пропущено: ${skipped}.${examples ? ` ${examples}${skipped > 3 ? "; …" : ""}` : ""}`);
  }
  $("#backlog-recommend-message").textContent = summary.join(" ");
  $("#backlog-recommend-message").classList.toggle("error", !state.llmItems.length && skipped > 0);
  updateRecommendationSortIndicators();
  showProviderWarnings(state.llmItems);
}

function renderApiRecommendations() {
  $("#backlog-recommend-body").innerHTML = state.apiItems.length
    ? sortedRecommendationEntries(state.apiItems).map(({item,index}) => (state.contentType === "music" ? recommendationAlbumRow : recommendationMovieRow)(item,index)).join("")
    : `<tr><td colspan="10" class="empty">Новых ${state.contentType === "music" ? "альбомов" : "фильмов"} по выбранным условиям нет</td></tr>`;
  $("#backlog-recommend-message").textContent = `Найдено новых ${state.contentType === "music" ? "альбомов" : "фильмов"}: ${state.apiItems.length}.`;
  $("#backlog-recommend-message").classList.remove("error");
  updateRecommendationSortIndicators();
  showProviderWarnings(state.apiItems);
}

function favoriteButton(item) {
  const active = Boolean(item.favorite);
  return `<button class="favorite-button ${active ? "active" : ""}" data-favorite-toggle data-id="${escapeHtml(item.id)}" aria-pressed="${active}" title="${active ? "Убрать из избранного" : "Добавить в избранное"}" aria-label="${active ? "Убрать из избранного" : "Добавить в избранное"}">${active ? "★" : "☆"}</button>`;
}

function ratedCard(item) {
  const reaction = item.reaction || "";
  const subtitle = state.contentType === "music" ? `${text(item.artists)} · ${text(item.year)}` : `${text(item.directors)} · ${text(item.year)}`;
  return `<article class="rated-card" data-card-id="${escapeHtml(item.id)}" tabindex="0">
    <div class="rated-card-main"><div><h3>${escapeHtml(title(item))}</h3><span class="rated-original">${escapeHtml(item.title_original)}</span><p>${escapeHtml(subtitle)}</p></div>${state.contentType === "music" ? albumCoverHtml(item,"rated-card-cover") : moviePosterHtml(item,"rated-card-poster")}</div>
    <div class="rated-actions">
      <button class="mini-button like" data-reaction-toggle="like" data-id="${escapeHtml(item.id)}">${reaction === "like" ? "Убрать лайк" : "Нравится"}</button>
      <button class="mini-button dislike" data-reaction-toggle="dislike" data-id="${escapeHtml(item.id)}">${reaction === "dislike" ? "Убрать дизлайк" : "Не понравилось"}</button>
      <button class="mini-button" data-item-action="backlog" data-id="${escapeHtml(item.id)}">В бэклог</button>
      ${state.contentType === "movie" ? favoriteButton(item) : ""}
    </div>
  </article>`;
}

function favoriteCard(item) {
  return `<article class="rated-card favorite-card" data-card-id="${escapeHtml(item.id)}" tabindex="0">
    <div class="favorite-card-head"><div><h3>${escapeHtml(title(item))}</h3><span class="rated-original">${escapeHtml(item.title_original)}</span></div>${moviePosterHtml(item,"rated-card-poster")}${favoriteButton(item)}</div>
    <p>${escapeHtml(text(item.directors))} · ${escapeHtml(text(item.year))}</p>
  </article>`;
}

function renderLibrary() {
  const backlog = sortedBacklog(filtered(state.items.filter(item => item.status === "backlog")));
  if (state.contentType === "music") {
    $("#backlog-head").innerHTML = `<tr><th>Обложка</th><th><button class="sort-button" data-sort="title_ru">Название</button></th><th><button class="sort-button" data-sort="artists">Исполнитель</button></th><th><button class="sort-button" data-sort="release_date">Первый выпуск</button></th><th><button class="sort-button" data-sort="track_count">Песен</button></th><th><button class="sort-button" data-sort="primary_type">Тип</button></th><th><button class="sort-button" data-sort="genres">Жанр</button></th><th><button class="sort-button" data-sort="total_listen_count">Прослушивания</button></th><th>Link</th><th></th></tr>`;
    $("#backlog-body").innerHTML = backlog.map(albumRow).join("");
  } else {
    $("#backlog-head").innerHTML = `<tr><th>Постер</th><th><button class="sort-button" data-sort="title_ru">Название</button></th><th><button class="sort-button" data-sort="release_date">Дата выхода</button></th><th><button class="sort-button" data-sort="directors">Режиссёр</button></th><th><button class="sort-button" data-sort="imdb_rating">IMDb / КП</button></th><th><button class="sort-button" data-sort="countries">Страны</button></th><th><button class="sort-button" data-sort="genres">Жанр</button></th><th><button class="sort-button" data-sort="key_people">Ключевые персоны</button></th><th>Link</th><th></th></tr>`;
    $("#backlog-body").innerHTML = backlog.map(movieRow).join("");
  }
  $("#backlog-count").textContent = backlog.length; $("#backlog-empty").classList.toggle("hidden", backlog.length > 0); updateSortIndicators();
  const consumed = filtered(state.items.filter(item => item.status === "consumed"));
  const groups = { unrated: consumed.filter(x => !x.reaction), liked: consumed.filter(x => x.reaction === "like"), disliked: consumed.filter(x => x.reaction === "dislike") };
  for (const [key, items] of Object.entries(groups)) {
    $(`#${key}-list`).innerHTML = items.length ? items.map(ratedCard).join("") : `<div class="empty">Пусто</div>`;
    $(`#${key}-count`).textContent = items.length;
  }
  const favorites = state.contentType === "movie" ? filtered(state.items.filter(item => item.favorite)) : [];
  $("#favorites-list").innerHTML = favorites.map(favoriteCard).join("");
  $("#favorites-count").textContent = favorites.length;
  $("#favorites-empty").classList.toggle("hidden", favorites.length > 0);
}

function showDetails(item) {
  if (!item) return;
  state.currentDetailId = state.items.some(existing => existing.id === item.id) ? item.id : "";
  $("#refresh-detail").classList.toggle("hidden", !state.currentDetailId);
  $("#details-title").textContent = `${title(item)}${item.year ? ` (${item.year})` : ""}`;
  if ((item.content_type || state.contentType) === "music") {
    $("#details-eyebrow").textContent = "Карточка альбома";
    $("#refresh-detail").textContent = "↻ Актуализировать из MusicBrainz";
    const fields = [["Исполнитель",item.artists],["Оригинальное название",item.title_original],["Первый выпуск",item.first_release_date || item.year],["Количество песен",item.track_count],["Тип",albumTypes(item)],["Жанры",item.genres],["Теги",item.tags],["Прослушивания ListenBrainz",listenCount(item)],["Страна издания",item.country],["Лейбл",item.label],["Каталожный номер",item.catalog_number],["Штрихкод",item.barcode],["Формат",item.media_formats],["Статус издания",item.release_status],["Уточнение",item.disambiguation],["Трек-лист",formatTrackList(item.track_list),true],["Аннотация",item.annotation,true],["Заметка",item.notes,true],["Raw · исходный ввод",formatRaw(item.raw_json),true]];
    const cards = fields.map(([label,value,wide]) => `<div class="detail ${wide ? "wide" : ""}"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
    const cover = `<div class="detail wide"><span>Обложка</span><div class="album-detail-artwork">${albumCoverHtml(item,"detail-cover")}${youtubeMusicAlbumLinkHtml(item)}</div></div>`;
    $("#details-content").innerHTML = cover + cards + `<div class="detail wide"><span>Ссылки</span><p class="detail-links">${albumLinksHtml(item)}</p></div>`;
    if (!$("#details-dialog").open) $("#details-dialog").showModal();
    return;
  }
  $("#details-eyebrow").textContent = "Карточка фильма";
  $("#refresh-detail").textContent = "↻ Актуализировать из TMDB";
  const fields = [
    ["Оригинальное название", item.title_original], ["Дата выхода", item.release_date || item.year], ["Режиссёр", item.directors], ["Жанры", item.genres],
    ["Длительность", item.duration_minutes ? `${item.duration_minutes} мин.` : ""], ["IMDb rating", item.imdb_rating], ["Кинопоиск rating", item.kinopoisk_rating], ["Голоса IMDb", item.imdb_votes], ["Metascore", item.metascore],
    ["TMDB rating", item.tmdb_rating ? `${item.tmdb_rating} (${text(item.tmdb_vote_count)} голосов)` : ""], ["TMDB ID", item.tmdb_id], ["Кинопоиск ID", item.kinopoisk_id], ["Статус", item.movie_status], ["Страны", item.countries],
    ["Сценаристы", item.writers], ["Актёры", item.cast, true], ["Ключевые персоны", formatKeyPeople(item), true],
    ["Награды", awardsSummary(item.awards_json), true], ["Ключевые слова", item.keywords, true], ["Описание", item.overview, true], ["Заметка", item.notes, true], ["Raw · исходный ввод", formatRaw(item.raw_json), true]
  ];
  const cards = fields.map(([label,value,wide]) => `<div class="detail ${wide ? "wide" : ""}"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
  const links = `<div class="detail wide"><span>Ссылки</span><p class="detail-links">${movieLinksHtml(item, true)}${validUrl(item.homepage) ? ` <a href="${escapeHtml(item.homepage)}" target="_blank" rel="noreferrer">Сайт ↗</a>` : ""}</p></div>`;
  const poster = item.poster_local_path || item.poster_path || validUrl(item.poster_url) ? `<div class="detail wide"><span>Постер</span>${moviePosterHtml(item,"detail-poster")}</div>` : "";
  $("#details-content").innerHTML = poster + cards + links;
  if (!$("#details-dialog").open) $("#details-dialog").showModal();
}

async function patchItem(id, changes) {
  const {item} = await request(`/api/library/${encodeURIComponent(id)}`, {method:"PATCH", body:JSON.stringify(changes)});
  state.items = state.items.map(existing => existing.id === item.id ? item : existing); renderLibrary(); toast(state.contentType === "music" ? "Музыкальная библиотека обновлена" : "Фильмотека обновлена");
}

async function toggleFavorite(button) {
  const item = state.items.find(movie => movie.id === button.dataset.id);
  if (!item) return;
  button.disabled = true;
  try {
    const result = await request(`/api/library/${encodeURIComponent(item.id)}/favorite`, {
      method:"POST", body:JSON.stringify({favorite:!item.favorite}),
    });
    state.items = state.items.map(movie => movie.id === result.item.id ? result.item : movie);
    renderLibrary(); toast(result.item.favorite ? "Добавлено в избранное" : "Удалено из избранного");
  } catch (error) { button.disabled = false; toast(error.message); }
}

function sortedPeople(items, role) {
  const setting = state.peopleSort[role];
  const numeric = new Set(["tmdb_id"]);
  return [...items].sort((left,right) => {
    const a=left[setting.key] ?? "",b=right[setting.key] ?? "";
    const result=numeric.has(setting.key) ? (Number(a)||-Infinity)-(Number(b)||-Infinity) : String(a).localeCompare(String(b),"ru",{numeric:true,sensitivity:"base"});
    return setting.direction === "asc" ? result : -result;
  });
}

function updatePeopleSortIndicators() {
  document.querySelectorAll("[data-people-sort]").forEach(button => {
    const setting=state.peopleSort[button.dataset.peopleRole],active=setting?.key===button.dataset.peopleSort;
    button.classList.toggle("active",active);button.dataset.direction=active?setting.direction:"";
    button.setAttribute("aria-sort",active?(setting.direction==="asc"?"ascending":"descending"):"none");
  });
}

function renderPeople() {
  const query = state.query.trim().toLocaleLowerCase("ru");
  const people = state.interests.filter(item => !query || [item.name_ru,item.name_original,item.tmdb_id,item.mbid].join(" ").toLocaleLowerCase("ru").includes(query));
  if (state.contentType === "music") {
    const artists=sortedPeople(people,"artist");
    $("#movie-people-board").classList.add("hidden"); $("#music-people-board").classList.remove("hidden");
    $("#artists-body").innerHTML = artists.length ? artists.map(item => `<tr class="person-row" data-person-details-id="${escapeHtml(item.id)}" tabindex="0"><td><strong>${escapeHtml(text(item.name_original))}</strong></td><td>${escapeHtml(text(item.artist_type))}</td><td>${escapeHtml(text(item.country || item.area))}</td><td>${escapeHtml(text(item.mbid))}</td><td><button class="icon-button trash-button" data-trash-entity="music_artist" data-id="${escapeHtml(item.id)}" title="В корзину" aria-label="Переместить исполнителя в корзину">🗑</button></td></tr>`).join("") : `<tr><td colspan="5" class="empty">Ничего не найдено</td></tr>`;
    $("#artists-count").textContent = artists.length; $("#people-count").textContent = artists.length; updatePeopleSortIndicators();
    return;
  }
  $("#movie-people-board").classList.remove("hidden"); $("#music-people-board").classList.add("hidden");
  for (const role of ["director", "actor"]) {
    const rows = sortedPeople(people.filter(item => item.role === role),role);
    $(`#${role}s-body`).innerHTML = rows.length ? rows.map(item => `<tr class="person-row" data-person-details-id="${escapeHtml(item.id)}" tabindex="0" aria-label="Открыть карточку: ${escapeHtml(titlePerson(item))}"><td><strong>${escapeHtml(text(item.name_ru))}</strong></td><td>${escapeHtml(text(item.name_original))}</td><td>${escapeHtml(text(item.tmdb_id))}</td><td><button class="icon-button trash-button" data-trash-entity="person" data-id="${escapeHtml(item.id)}" data-role="${role}" title="В корзину" aria-label="Переместить персону в корзину">🗑</button></td></tr>`).join("") : `<tr><td colspan="4" class="empty">Ничего не найдено</td></tr>`;
    $(`#${role}s-count`).textContent = rows.length;
  }
  $("#people-count").textContent = people.length; updatePeopleSortIndicators();
}

function showPersonDetails(person) {
  if (!person) return;
  state.currentPersonId = person.id;
  $("#person-details-title").textContent = titlePerson(person);
  if ((person.content_type || state.contentType) === "music") {
    $("#person-details-role").textContent = "Карточка исполнителя";
    $("#refresh-person-detail").textContent = "↻ Актуализировать из MusicBrainz";
    const fields = [["Исполнитель",person.name_original],["Тип",person.artist_type],["Страна",person.country],["Регион",person.area],["Период",[person.life_span_begin,person.life_span_end].filter(Boolean).join(" — ")],["Уточнение",person.disambiguation],["MusicBrainz ID",person.mbid],["Raw · исходный ввод",formatRaw(person.raw_json)]];
    $("#person-details-content").innerHTML = fields.map(([label,value]) => `<div class="detail"><span>${label}</span><p>${escapeHtml(text(value))}</p></div>`).join("");
    if (!$("#person-details-dialog").open) $("#person-details-dialog").showModal();
    return;
  }
  $("#refresh-person-detail").textContent = "↻ Актуализировать из TMDB";
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
  const movies = state.trash.filter(item => ["movie","album"].includes(item.entity_type));
  const people = state.trash.filter(item => ["person","music_artist"].includes(item.entity_type));
  const restore = item => `<button class="mini-button" data-restore-id="${escapeHtml(item.id)}">Восстановить</button>`;
  $("#trash-movies-body").innerHTML = movies.length ? movies.map(item => `<tr><td><strong>${escapeHtml(text(item.title_ru))}</strong></td><td>${escapeHtml(text(item.title_original))}${item.entity_type === "album" && item.artists ? ` · ${escapeHtml(item.artists)}` : ""}</td><td>${escapeHtml(text(item.year))}</td><td>${restore(item)}</td></tr>`).join("") : `<tr><td colspan="4" class="empty">Корзина контента пуста</td></tr>`;
  $("#trash-people-body").innerHTML = people.length ? people.map(item => `<tr><td><strong>${escapeHtml(text(item.name_ru))}</strong></td><td>${escapeHtml(text(item.name_original))}</td><td>${item.entity_type === "music_artist" ? "Исполнитель" : item.role === "director" ? "Режиссёр" : "Актёр"}</td><td>${restore(item)}</td></tr>`).join("") : `<tr><td colspan="4" class="empty">Корзина персон пуста</td></tr>`;
  $("#trash-movies-count").textContent = movies.length; $("#trash-people-count").textContent = people.length; $("#trash-count").textContent = state.trash.length;
  $("#clear-trash").disabled = state.trash.length === 0;
}

function switchView(view) {
  state.view = view; document.querySelectorAll("[data-view]").forEach(button => button.classList.toggle("active", button.dataset.view === view));
  $("#library-view").classList.toggle("hidden", !["backlog", "consumed"].includes(view));
  $("#favorites-view").classList.toggle("hidden", view !== "favorites");
  $("#people-view").classList.toggle("hidden", view !== "people"); $("#backlog-panel").classList.toggle("hidden", view !== "backlog"); $("#consumed-panel").classList.toggle("hidden", view !== "consumed");
  $("#trash-view").classList.toggle("hidden", view !== "trash");
  $(".content-choice").classList.toggle("hidden", ["favorites", "trash"].includes(view)); $("#open-add").classList.toggle("hidden", view !== "backlog"); $("#open-add-person").classList.toggle("hidden", view !== "people");
  $("#search").closest(".search").classList.toggle("hidden", view === "trash"); $("#search").placeholder = view === "people" ? (state.contentType === "music" ? "Найти исполнителя" : "Найти персону") : (state.contentType === "music" ? "Найти альбом" : "Найти фильм");
  $("#page-title").textContent = ({backlog:"Бэклог", consumed:"Просмотрено", favorites:"Избранное", people:"Персоны", trash:"Корзина"})[view]; renderLibrary(); renderPeople(); renderTrash();
}

function renderInterestOptions() {
  if (state.contentType === "music") {
    $("#artist-options").innerHTML = state.interests.map(item => `<label><input type="checkbox" name="artist_ids" value="${escapeHtml(item.id)}" checked> ${escapeHtml(item.name_original)}</label>`).join("");
    const all = document.querySelector('[data-select-all="artist"]'); if (all) all.checked = state.interests.length > 0;
    filterPersonOptions("artist"); updateSelectedLabels(); return;
  }
  for (const role of ["actor","director"]) {
    const items = state.interests.filter(item => item.role === role);
    $(`#${role}-options`).innerHTML = items.map(item => `<label><input type="checkbox" name="${role}_ids" value="${escapeHtml(item.id)}" checked> ${escapeHtml(item.name_original)}</label>`).join("");
    document.querySelector(`[data-select-all="${role}"]`).checked = true;
    filterPersonOptions(role);
  }
  updateSelectedLabels();
}

function filterPersonOptions(role) {
  const input = document.querySelector(`[data-person-search="${role}"]`), query = String(input?.value || "").trim().toLocaleLowerCase("ru");
  document.querySelectorAll(`#${role}-options > label`).forEach(label => {
    const id = label.querySelector("input")?.value, person = state.interests.find(item => String(item.id) === String(id));
    const haystack = [person?.name_ru,person?.name_original,person?.name].filter(Boolean).join(" ").toLocaleLowerCase("ru");
    label.classList.toggle("hidden", Boolean(query) && !haystack.includes(query));
  });
}

function syncPeopleMenuLayout() {
  const dialog = $("#backlog-recommend-dialog"), open = Boolean(dialog.querySelector(".people-filter details[open]"));
  dialog.classList.toggle("people-menu-open",open);
}

function updateSelectedLabels() {
  for (const role of ["actor","director","artist"]) {
    const boxes = [...document.querySelectorAll(`input[name="${role}_ids"]`)], selected = boxes.filter(box => box.checked).length;
    const label = $(`#${role}s-selected`); if (label) label.textContent = `· ${selected}/${boxes.length}`; const all = document.querySelector(`[data-select-all="${role}"]`);
    if (!all) continue;
    all.checked = selected === boxes.length && boxes.length > 0; all.indeterminate = selected > 0 && selected < boxes.length;
  }
}

function applyContentLabels() {
  const music = state.contentType === "music";
  document.querySelectorAll("[data-content-type]").forEach(button=>{const active=button.dataset.contentType===state.contentType;button.classList.toggle("active",active);button.setAttribute("aria-pressed",String(active));});
  document.querySelector('[data-view="favorites"]').classList.toggle("hidden", music);
  $("#backlog-heading").textContent = music ? "Альбомы в очереди" : "Фильмы в очереди";
  $("#backlog-empty").textContent = music ? "В музыкальном бэклоге ничего нет." : "В бэклоге ничего нет.";
  $("#open-backlog-recommend").textContent = music ? "✦ Порекомендовать альбомы" : "✦ Порекомендовать фильмы";
  $("#refresh-tmdb").textContent = music ? "↻ Актуализировать из MusicBrainz" : "↻ Актуализировать из TMDB";
  $("#refresh-people").textContent = music ? "↻ Актуализировать из MusicBrainz" : "↻ Актуализировать из TMDB";
  $("#open-add").textContent = music ? "+ Добавить альбом" : "+ Добавить фильм";
  $("#open-add-person").textContent = music ? "+ Добавить исполнителя" : "+ Добавить персону";
  $("#search").placeholder = state.view === "people" ? (music ? "Найти исполнителя" : "Найти персону") : (music ? "Найти альбом" : "Найти фильм");
}

async function changeContentType(contentType) {
  state.contentType = contentType; state.query = ""; $("#search").value = "";
  state.sortKey = "release_date"; state.sortDirection = "desc";
  const [library, people] = await Promise.all([request(`/api/library?content_type=${encodeURIComponent(contentType)}`), request(`/api/people?content_type=${encodeURIComponent(contentType)}`)]);
  state.items = library.items; state.interests = people.items; applyContentLabels(); renderLibrary(); renderPeople(); renderInterestOptions();
}

function recommendationValues(form) {
  const data = new FormData(form);
  if (state.contentType === "music") {
    const values={artist_ids:data.getAll("artist_ids"),limit:data.get("limit")};
    for(const name of ["year_from","year_to"]) if(data.has(name)) values[name]=data.get(name);
    if(data.has("excluded_types")) values.excluded_types=String(data.get("excluded_types")||"").split(",").map(value=>value.trim()).filter(Boolean);
    return values;
  }
  const values = { actor_ids:data.getAll("actor_ids"), director_ids:data.getAll("director_ids"), limit:data.get("limit") };
  for (const name of ["min_imdb_rating", "min_kinopoisk_rating", "min_votes", "min_runtime", "year_from", "year_to", "excluded_genres"]) {
    if (data.has(name)) values[name] = data.get(name);
  }
  return values;
}

async function addModalRecommendation(index, action, button) {
  const items = state.backlogRecommendMode === "llm" ? state.llmItems : state.apiItems;
  const source = items[index];
  if (!source) return;
  if (action === "remove") {
    items.splice(index, 1);
    if (state.backlogRecommendMode === "llm") renderLlmRecommendations(); else renderApiRecommendations();
    return;
  }
  button.disabled = true;
  const payload = {...source};
  if (action === "backlog") { payload.status = "backlog"; payload.reaction = ""; }
  else { payload.status = "consumed"; payload.reaction = action; }
  try {
    const {item} = await request("/api/library", {method:"POST", body:JSON.stringify(payload)});
    state.items.push(item); items.splice(index, 1);
    if (state.backlogRecommendMode === "llm") renderLlmRecommendations(); else renderApiRecommendations();
    renderLibrary();
    toast(action === "backlog" ? "Добавлено в бэклог" : "Добавлено в просмотренное");
  } catch (error) { button.disabled = false; toast(error.message); }
}

async function refreshTmdb() {
  const button = $("#refresh-tmdb"), notice = $("#refresh-progress"), music = state.contentType === "music"; button.disabled = true; notice.classList.remove("hidden","error","warning"); notice.textContent = music ? "Полностью актуализирую неполные карточки через MusicBrainz и ListenBrainz…" : "Полностью актуализирую неполные карточки через нужные источники…";
  try { const result = await request(music ? "/api/library/refresh-musicbrainz" : "/api/library/refresh-tmdb", {method:"POST",body:"{}"}); state.items = (await request(`/api/library?content_type=${state.contentType}`)).items; renderLibrary(); const warnings = providerWarnings([{provider_warnings:result.provider_warnings || []}]); notice.textContent = `Обновлено ${result.updated} из ${result.total}. Ошибок: ${result.failed}.${warnings.length ? ` ${warnings.join(" ")}` : ""}`; if (result.failed) notice.classList.add("error"); else if (warnings.length) notice.classList.add("warning"); if (warnings.length) toast(warnings[0]); }
  catch (error) { notice.textContent = error.message; notice.classList.add("error"); } finally { button.disabled = false; }
}

async function refreshPeople() {
  const button = $("#refresh-people"), notice = $("#people-refresh-progress"), music = state.contentType === "music"; button.disabled = true; notice.classList.remove("hidden","error"); notice.textContent = music ? "Сверяю исполнителей с MusicBrainz…" : "Сверяю имена и TMDB ID…";
  try { const result = await request(music ? "/api/people/refresh-musicbrainz" : "/api/people/refresh-tmdb", {method:"POST",body:"{}"}); state.interests = (await request(`/api/people?content_type=${state.contentType}`)).items; renderPeople(); renderInterestOptions(); notice.textContent = `Обновлено ${result.updated} из ${result.total}. Ошибок: ${result.failed}.`; if (result.failed) notice.classList.add("error"); }
  catch (error) { notice.textContent = error.message; notice.classList.add("error"); } finally { button.disabled = false; }
}

async function refreshCurrentMovie() {
  if (!state.currentDetailId) return;
  const button = $("#refresh-detail"); button.disabled = true; button.textContent = "Обновляю…";
  try {
    const music = state.contentType === "music";
    const {item} = await request(`/api/library/${encodeURIComponent(state.currentDetailId)}/${music ? "refresh-musicbrainz" : "refresh-tmdb"}`, {method:"POST",body:"{}"});
    state.items = state.items.map(existing => existing.id === item.id ? item : existing); renderLibrary(); showDetails(item);
    const warnings = providerWarnings([item]); toast(warnings[0] || (item.refresh_skipped ? "Все необходимые данные уже заполнены" : `Карточка обновлена из ${music ? "MusicBrainz / ListenBrainz" : "TMDB"}`));
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = state.contentType === "music" ? "↻ Актуализировать из MusicBrainz" : "↻ Актуализировать из TMDB"; }
}

async function refreshCurrentPerson() {
  if (!state.currentPersonId) return;
  const button = $("#refresh-person-detail"); button.disabled = true; button.textContent = "Обновляю…";
  try {
    const music = state.contentType === "music";
    const {item} = await request(`/api/people/${encodeURIComponent(state.currentPersonId)}/${music ? "refresh-musicbrainz" : "refresh-tmdb"}`, {method:"POST",body:"{}"});
    state.interests = state.interests.map(existing => existing.id === item.id ? item : existing);
    renderPeople(); renderInterestOptions(); showPersonDetails(item); toast(music ? "Исполнитель обновлён из MusicBrainz" : "Персона обновлена из TMDB");
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = `↻ Актуализировать из ${state.contentType === "music" ? "MusicBrainz" : "TMDB"}`; }
}

function formPayload(form) { return Object.fromEntries(new FormData(form).entries()); }
function llmRecommendationPayload(form) {
  return {
    ...formPayload(form),
    disabled_filters: [...form.querySelectorAll("[data-filter-field] input:disabled")].map(input => input.name),
  };
}
function fillForm(form, values, fields) {
  for (const field of fields) {
    const control = form.elements.namedItem(field);
    if (control && values[field] !== undefined && values[field] !== null) control.value = values[field];
  }
}
function resolveStatus(selector, message = "", isError = false, isWarning = false) {
  const node = $(selector); node.textContent = message; node.classList.toggle("hidden", !message); node.classList.toggle("error", isError); node.classList.toggle("warning", isWarning);
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
    const warnings = providerWarnings([item]);
    resolveStatus("#add-resolve-status", `Найдено: ${item.title_ru || item.title_original}${item.year ? ` (${item.year})` : ""}. Исходный ввод сохранится в Raw.${warnings.length ? ` ${warnings.join(" ")}` : ""}`, false, warnings.length > 0);
    if (warnings.length) toast(warnings[0]);
  } catch (error) { resolveStatus("#add-resolve-status", error.message, true); }
  finally { button.disabled = false; button.textContent = "↻ Актуализировать из TMDB"; }
}

function resetAlbumDraft() {
  $("#add-album-form").reset(); state.pendingAlbumDetails = null; state.pendingAlbumRaw = null; state.pendingAlbumSearchField = "";
  $("#add-album-links").textContent = "—"; renderAlbumDraftPreview(); resolveStatus("#add-album-resolve-status"); $("#add-album-error").textContent = "";
}

async function saveAlbumDraft(reaction = "") {
  const form = $("#add-album-form"), current = formPayload(form), status = reaction ? "consumed" : "backlog";
  const payload = {...(state.pendingAlbumDetails || {}),...current,title_ru:current.title_original,raw_data:state.pendingAlbumRaw || compactObject(current),content_type:"music",status,reaction};
  const buttons = form.querySelectorAll("[data-save-album]"); buttons.forEach(button => { button.disabled = true; });
  try { const {item}=await request("/api/library",{method:"POST",body:JSON.stringify(payload)});state.items.push(item);resetAlbumDraft();$("#add-album-dialog").close();renderLibrary();toast(reaction === "like" ? "Альбом добавлен в понравившееся" : reaction === "dislike" ? "Альбом добавлен в не понравившееся" : "Альбом добавлен в бэклог"); }
  catch(error){$("#add-album-error").textContent=error.message;} finally{buttons.forEach(button=>{button.disabled=false;});}
}

async function resolveAlbumDraft() {
  const form=$("#add-album-form"),button=$("#resolve-album"),draft=formPayload(form);state.pendingAlbumRaw??=compactObject(draft);button.disabled=true;button.textContent="Ищу…";$("#add-album-error").textContent="";
  try { const {item}=await request("/api/resolve/album",{method:"POST",body:JSON.stringify(draft)});state.pendingAlbumDetails=item;fillForm(form,item,["title_original","artists","year","release_group_mbid"]);$("#add-album-links").innerHTML=albumLinksHtml(item);renderAlbumDraftPreview(item);const warnings=providerWarnings([item]);resolveStatus("#add-album-resolve-status",`Найдено: ${item.artists} — ${item.title_original}${item.year ? ` (${item.year})` : ""}. Исходный ввод сохранится в Raw.${warnings.length?` ${warnings.join(" ")}`:""}`,false,warnings.length>0);if(warnings.length)toast(warnings[0]); }
  catch(error){resolveStatus("#add-album-resolve-status",error.message,true);} finally{button.disabled=false;button.textContent="↻ Актуализировать из MusicBrainz";}
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

async function resolveArtistDraft() {
  const form=$("#add-artist-form"),button=$("#resolve-artist"),draft={...formPayload(form),content_type:"music"};state.pendingArtistRaw??=compactObject(draft);button.disabled=true;button.textContent="Ищу…";$("#add-artist-error").textContent="";
  try { const {item}=await request("/api/resolve/person",{method:"POST",body:JSON.stringify(draft)});state.pendingArtistDetails=item;fillForm(form,item,["name","mbid"]);resolveStatus("#add-artist-resolve-status",`Найдено: ${item.name_original}. Исходный ввод сохранится в Raw.`); }
  catch(error){resolveStatus("#add-artist-resolve-status",error.message,true);} finally{button.disabled=false;button.textContent="↻ Актуализировать из MusicBrainz";}
}

async function reloadActiveData() {
  const [library, people, trash] = await Promise.all([request(`/api/library?content_type=${state.contentType}`), request(`/api/people?content_type=${state.contentType}`), request("/api/trash")]);
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

async function clearTrash(button) {
  const count = state.trash.length;
  if (!count || !window.confirm(`Удалить навсегда все объекты из корзины (${count})? Это действие нельзя отменить.`)) return;
  button.disabled = true; button.textContent = "Очищаю…";
  try {
    const result = await request("/api/trash/empty", {method:"POST", body:"{}"});
    await reloadActiveData();
    const warning = result.artwork_errors?.length ? ` Не удалось удалить файлов: ${result.artwork_errors.length}.` : "";
    toast(`Корзина очищена: удалено ${result.deleted}.${warning}`);
  } catch (error) { button.disabled = false; toast(error.message); }
  finally { button.textContent = "Очистить корзину"; }
}

function renderRecommendationHead() {
  const sort = (label,key) => `<button class="sort-button" type="button" data-recommend-sort="${key}">${label}</button>`;
  const notes = state.backlogRecommendMode === "llm" ? `<th>${sort("Заметка","notes")}</th>` : "";
  $("#backlog-recommend-head").innerHTML = state.contentType === "music"
    ? `<tr><th>Обложка</th><th>${sort("Название","title")}</th><th>${sort("Исполнитель","artists")}</th><th>${sort("Первый выпуск","year")}</th><th>${sort("Песен","track_count")}</th><th>${sort("Тип","types")}</th><th>${sort("Жанр","genres")}</th><th>${sort("Прослушивания","total_listen_count")}</th>${notes}<th>Link</th><th>Действия</th></tr>`
    : `<tr><th>Постер</th><th>${sort("Название","title")}</th><th>${sort("Дата выхода","year")}</th><th>${sort("Режиссёр","directors")}</th><th>${sort("IMDb / КП","rating")}</th><th>${sort("Страны","countries")}</th><th>${sort("Жанр","genres")}</th><th>${sort("Ключевые персоны","key_people")}</th>${notes}<th>Link</th><th>Действия</th></tr>`;
  updateRecommendationSortIndicators();
}

function setBacklogRecommendMode(mode) {
  state.backlogRecommendMode = mode;
  document.querySelectorAll("[data-backlog-recommend-mode]").forEach(button => button.classList.toggle("active", button.dataset.backlogRecommendMode === mode));
  const music = state.contentType === "music";
  $("#backlog-recommend-form").classList.toggle("hidden", music || mode !== "llm");
  $("#backlog-api-form").classList.toggle("hidden", music || mode !== "api");
  $("#album-recommend-form").classList.toggle("hidden", !music || mode !== "llm");
  $("#album-api-form").classList.toggle("hidden", !music || mode !== "api");
  $("#api-recommend-progress").classList.add("hidden");
  state.apiProgressWarnings = [];
  renderRecommendationHead();
  $("#backlog-recommend-result").classList.add("hidden");
  $("#backlog-recommend-warning").classList.add("hidden");
  $("#backlog-recommend-error").textContent = ""; $("#backlog-api-error").textContent = ""; $("#album-recommend-error").textContent = ""; $("#album-api-error").textContent = "";
}

function openBacklogRecommendation() {
  const currentYear = new Date().getFullYear();
  for (const yearTo of [$("#backlog-year-to"), $("#api-year-to"), $("#album-llm-year-to"), $("#album-api-year-to")]) {
    yearTo.max = String(currentYear + 2); yearTo.value = String(currentYear);
  }
  state.llmItems = []; state.apiItems = []; state.llmErrors = []; state.apiProgressWarnings = []; state.recommendationSort = {key:"year",direction:"desc"};
  $("#backlog-recommend-error").textContent = ""; $("#backlog-recommend-result").classList.add("hidden"); $("#backlog-recommend-body").innerHTML = ""; $("#backlog-recommend-message").textContent = "";
  const provider = state.llm.provider || "Codex SDK", model = state.llm.model ? ` · ${state.llm.model}` : "";
  $("#backlog-recommend-provider").textContent = `${provider}${model}${state.llm.configured === false ? " · требуется установка" : ""}`;
  $("#backlog-api-provider").textContent = state.tmdb.configured ? `TMDB · подключён${state.tmdb.omdb_configured ? " · OMDb" : ""}${state.tmdb.kinopoisk_configured ? " · КП" : ""}` : "TMDB · нужен ключ";
  $("#recommend-title").textContent = state.contentType === "music" ? "Порекомендовать альбомы" : "Порекомендовать фильмы";
  const apiModeButton = document.querySelector('[data-backlog-recommend-mode="api"]'); apiModeButton.textContent = state.contentType === "music" ? "Рекомендации по API MusicBrainz" : "Рекомендации по API TMDB";
  setBacklogRecommendMode("llm");
  $("#backlog-recommend-dialog").showModal();
}

async function runBacklogRecommendation(event) {
  event.preventDefault();
  const music = state.contentType === "music", button = $(music ? "#album-recommend-submit" : "#backlog-recommend-submit"), errorNode = $(music ? "#album-recommend-error" : "#backlog-recommend-error"), resultNode = $("#backlog-recommend-result");
  button.disabled = true; button.textContent = "Подбираю и уточняю…"; errorNode.textContent = ""; resultNode.classList.add("hidden"); $("#backlog-recommend-warning").classList.add("hidden");
  try {
    const payload = {...llmRecommendationPayload(event.currentTarget),content_type:state.contentType};
    const result = await request("/api/recommendations/llm", {method:"POST", body:JSON.stringify(payload)});
    state.llmItems = result.items || []; state.llmErrors = result.errors || []; renderLlmRecommendations();
    if (result.provider_warnings?.length) showProviderWarnings([{provider_warnings:result.provider_warnings}]);
    $("#backlog-recommend-result-source").textContent = music ? "Codex + MusicBrainz" : "Codex + TMDB + OMDb + Кинопоиск";
    $("#backlog-recommend-model").textContent = result.model || "Codex"; resultNode.classList.remove("hidden");
  } catch (error) { errorNode.textContent = error.message; }
  finally { button.disabled = false; button.textContent = "Порекомендовать"; }
}

let apiProgressTimer = null;
function renderApiProgress(value) {
  const stages = value.stages?.length ? value.stages : [{label:value.label || "API · персоны",processed:value.processed,total:value.total,unit:"персон"}];
  $("#api-recommend-progress").classList.remove("hidden");
  $("#api-recommend-progress-stages").innerHTML = stages.map(stage => {
    const total = Math.max(0,Number(stage.total)||0), processed = Math.min(total,Math.max(0,Number(stage.processed)||0));
    const percent = total ? Math.round(processed*100/total) : 100;
    return `<div class="recommend-progress-stage"><span><strong>${escapeHtml(stage.label || "API")}</strong> — обработано ${escapeHtml(stage.unit || "объектов")}: ${processed} из ${total} (${percent}%)</span><progress max="${Math.max(1,total)}" value="${total ? processed : 1}"></progress></div>`;
  }).join("");
}
function updateApiProgressWarnings(warnings = []) {
  const known = new Set(state.apiProgressWarnings.map(warning => `${warning.provider}\n${warning.message}`));
  const added = warnings.filter(warning => warning?.message && !known.has(`${warning.provider}\n${warning.message}`));
  for (const warning of added) {
    state.apiProgressWarnings.push(warning); known.add(`${warning.provider}\n${warning.message}`);
  }
  showProviderWarnings([]);
  if (added.length) toast(providerWarningText(added[0]));
}
async function pollApiProgress(progressId) {
  try {
    const response = await fetch(`/api/recommendations/progress?id=${encodeURIComponent(progressId)}`);
    if (!response.ok) return;
    const value = await response.json();
    if (value.found) { renderApiProgress(value); updateApiProgressWarnings(value.warnings || []); }
  } catch {}
}
function startApiProgress(progressId, total, label) {
  if (apiProgressTimer) clearInterval(apiProgressTimer);
  state.apiProgressWarnings = [];
  renderApiProgress({processed:0,total,label,stages:[]});
  apiProgressTimer = setInterval(() => pollApiProgress(progressId), 500);
}
async function finishApiProgress(progressId) {
  if (apiProgressTimer) clearInterval(apiProgressTimer);
  apiProgressTimer = null;
  await pollApiProgress(progressId);
}

async function runBacklogApiRecommendation(event) {
  event.preventDefault();
  const music = state.contentType === "music", button = $(music ? "#album-api-submit" : "#backlog-api-submit"), errorNode = $(music ? "#album-api-error" : "#backlog-api-error"), resultNode = $("#backlog-recommend-result");
  button.disabled = true; button.textContent = "Ищу и уточняю…"; errorNode.textContent = ""; resultNode.classList.add("hidden"); $("#backlog-recommend-warning").classList.add("hidden");
  let progressId = "";
  try {
    const values = recommendationValues(event.currentTarget), payload = {...values};
    progressId = globalThis.crypto?.randomUUID?.() || `recommend-${Date.now()}-${Math.random()}`;
    payload.progress_id = progressId;
    const personTotal = music ? values.artist_ids.length : values.actor_ids.length + values.director_ids.length;
    startApiProgress(progressId, personTotal, music ? "MusicBrainz · альбомы исполнителей" : "TMDB · фильмы персон");
    if (music) {
      const result = await request("/api/recommendations/musicbrainz", {method:"POST",body:JSON.stringify(payload)});
      state.apiItems = result.items || []; state.llmErrors = result.errors || []; renderApiRecommendations();
      if (result.provider_warnings?.length) showProviderWarnings([{provider_warnings:result.provider_warnings}]);
      $("#backlog-recommend-result-source").textContent = "MusicBrainz"; $("#backlog-recommend-model").textContent = "MusicBrainz API v2"; resultNode.classList.remove("hidden"); return;
    }
    if (values.year_from !== undefined) payload.date_from = `${values.year_from}-01-01`;
    if (values.year_to !== undefined) payload.date_to = `${values.year_to}-12-31`;
    delete payload.year_from;
    delete payload.year_to;
    if (values.excluded_genres !== undefined) payload.excluded_genres = String(values.excluded_genres).split(",").map(value => value.trim()).filter(Boolean);
    const {items} = await request("/api/recommendations/tmdb", {method:"POST", body:JSON.stringify(payload)});
    state.apiItems = items || []; renderApiRecommendations();
    $("#backlog-recommend-result-source").textContent = "TMDB + OMDb + Кинопоиск";
    $("#backlog-recommend-model").textContent = "TMDB API"; resultNode.classList.remove("hidden");
  } catch (error) { errorNode.textContent = error.message; }
  finally { if (progressId) await finishApiProgress(progressId); button.disabled = false; button.textContent = state.contentType === "music" ? "Найти альбомы" : "Найти фильмы"; }
}

document.addEventListener("click", async event => {
  const activeFilterDetails=event.target.closest(".people-filter details");document.querySelectorAll(".people-filter details[open]").forEach(details=>{if(details!==activeFilterDetails)details.removeAttribute("open");});
  const contentType=event.target.closest("[data-content-type]");if(contentType){if(contentType.dataset.contentType!==state.contentType){try{await changeContentType(contentType.dataset.contentType);}catch(error){toast(error.message);}}return;}
  const nav = event.target.closest("[data-view]"); if (nav) { switchView(nav.dataset.view); return; }
  const close = event.target.closest("[data-close-dialog]"); if (close) { $(`#${close.dataset.closeDialog}`).close(); return; }
  const sort = event.target.closest("[data-sort]"); if (sort) { const key=sort.dataset.sort; state.sortDirection = state.sortKey === key && state.sortDirection === "asc" ? "desc" : "asc"; state.sortKey=key; renderLibrary(); return; }
  const recommendationSort = event.target.closest("[data-recommend-sort]"); if (recommendationSort) { const key=recommendationSort.dataset.recommendSort,setting=state.recommendationSort;setting.direction=setting.key===key&&setting.direction==="asc"?"desc":"asc";setting.key=key;if(state.backlogRecommendMode==="llm")renderLlmRecommendations();else renderApiRecommendations();return; }
  const peopleSort=event.target.closest("[data-people-sort]");if(peopleSort){const role=peopleSort.dataset.peopleRole,key=peopleSort.dataset.peopleSort,setting=state.peopleSort[role];setting.direction=setting.key===key&&setting.direction==="asc"?"desc":"asc";setting.key=key;renderPeople();return;}
  const trash = event.target.closest("[data-trash-entity]"); if (trash) { await moveToTrash(trash); return; }
  const restore = event.target.closest("[data-restore-id]"); if (restore) { await restoreFromTrash(restore); return; }
  const clearTrashButton = event.target.closest("#clear-trash"); if (clearTrashButton) { await clearTrash(clearTrashButton); return; }
  const details = event.target.closest("[data-details-id]"); if (details) { showDetails(state.items.find(item=>item.id===details.dataset.detailsId)); return; }
  const personDetails = event.target.closest("[data-person-details-id]"); if (personDetails) { showPersonDetails(state.interests.find(item=>item.id===personDetails.dataset.personDetailsId)); return; }
  const modalRecDetails = event.target.closest("[data-modal-rec-details]"); if (modalRecDetails) { const items=state.backlogRecommendMode === "llm" ? state.llmItems : state.apiItems; showDetails(items[Number(modalRecDetails.dataset.modalRecDetails)]); return; }
  const itemAction = event.target.closest("[data-item-action]"); if (itemAction) { const action=itemAction.dataset.itemAction; await patchItem(itemAction.dataset.id,action === "backlog" ? {status:"backlog"} : {status:"consumed",reaction:action}); return; }
  const reaction = event.target.closest("[data-reaction-toggle]"); if (reaction) { const item=state.items.find(x=>x.id===reaction.dataset.id); const next=item.reaction===reaction.dataset.reactionToggle ? "" : reaction.dataset.reactionToggle; await patchItem(item.id,{reaction:next}); return; }
  const favorite = event.target.closest("[data-favorite-toggle]"); if (favorite) { await toggleFavorite(favorite); return; }
  const modalRecAction = event.target.closest("[data-modal-rec-action]"); if (modalRecAction) { await addModalRecommendation(Number(modalRecAction.dataset.index),modalRecAction.dataset.modalRecAction,modalRecAction); return; }
  const card = event.target.closest("[data-card-id]"); if (card) { showDetails(state.items.find(item=>item.id===card.dataset.cardId)); return; }
  const recommendMode = event.target.closest("[data-backlog-recommend-mode]"); if (recommendMode) { setBacklogRecommendMode(recommendMode.dataset.backlogRecommendMode); return; }
  const filterToggle = event.target.closest("[data-filter-toggle]"); if (filterToggle) { const field=filterToggle.closest("[data-filter-field]"); const input=field.querySelector("input"); input.disabled=!input.disabled; field.classList.toggle("filter-disabled",input.disabled); filterToggle.setAttribute("aria-pressed",String(!input.disabled)); filterToggle.textContent=input.disabled?"—":"✓"; filterToggle.setAttribute("aria-label",`${input.disabled?"Включить":"Отключить"} фильтр ${field.dataset.filterLabel}`); }
});

document.addEventListener("keydown", event => {
  if(event.key==="Escape")document.querySelectorAll(".people-filter details[open]").forEach(details=>details.removeAttribute("open"));
  if (event.key !== "Enter" && event.key !== " ") return;
  if (event.target.closest("button, a, input, select, textarea")) return;
  const card=event.target.closest("[data-card-id]"); if (card) { event.preventDefault(); showDetails(state.items.find(item=>item.id===card.dataset.cardId)); return; }
  const person=event.target.closest("[data-person-details-id]"); if (person) { event.preventDefault(); showPersonDetails(state.interests.find(item=>item.id===person.dataset.personDetailsId)); }
});
document.addEventListener("error",event=>{if(event.target.matches?.("[data-artwork]")){const placeholder=document.createElement("span");placeholder.className=event.target.classList.contains("movie-poster")||event.target.classList.contains("rated-card-poster")||event.target.classList.contains("detail-poster")?"movie-poster-placeholder":"album-cover-placeholder";placeholder.setAttribute("aria-hidden","true");event.target.replaceWith(placeholder);}},true);
document.addEventListener("change", event => { const all=event.target.closest("[data-select-all]"); if (all) document.querySelectorAll(`input[name="${all.dataset.selectAll}_ids"]`).forEach(box=>box.checked=all.checked); if (all || event.target.matches('input[name="actor_ids"],input[name="director_ids"],input[name="artist_ids"]')) updateSelectedLabels(); });
document.addEventListener("input",event=>{const search=event.target.closest("[data-person-search]");if(search)filterPersonOptions(search.dataset.personSearch);});
document.querySelectorAll(".people-filter details").forEach(details=>details.addEventListener("toggle",()=>{syncPeopleMenuLayout();if(details.open)requestAnimationFrame(()=>details.querySelector("[data-person-search]")?.focus());}));
document.querySelectorAll("dialog").forEach(dialog => dialog.addEventListener("click", event => { if (dialog.id === "backlog-recommend-dialog") return; const rect=dialog.getBoundingClientRect(); if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) dialog.close(); }));
$("#backlog-recommend-dialog").addEventListener("cancel", event => event.preventDefault());

$("#search").addEventListener("input", event => { state.query=event.target.value; renderLibrary(); renderPeople(); }); $("#refresh-tmdb").addEventListener("click", refreshTmdb); $("#refresh-people").addEventListener("click", refreshPeople); $("#refresh-detail").addEventListener("click", refreshCurrentMovie); $("#refresh-person-detail").addEventListener("click", refreshCurrentPerson);
$("#open-backlog-recommend").addEventListener("click", openBacklogRecommendation); $("#backlog-recommend-form").addEventListener("submit", runBacklogRecommendation); $("#album-recommend-form").addEventListener("submit", runBacklogRecommendation); $("#backlog-api-form").addEventListener("submit", runBacklogApiRecommendation); $("#album-api-form").addEventListener("submit", runBacklogApiRecommendation);
$("#open-add").addEventListener("click",()=>{ if(state.contentType === "music"){resetAlbumDraft();$("#add-album-dialog").showModal();$("#add-album-form").elements.namedItem("title_original").focus();}else{resetMovieDraft();$("#add-dialog").showModal();$("#add-form").elements.namedItem("title_ru").focus();} });
$("#open-add-person").addEventListener("click",()=>{ if(state.contentType === "music"){state.pendingArtistDetails=null;state.pendingArtistRaw=null;$("#add-artist-form").reset();resolveStatus("#add-artist-resolve-status");$("#add-artist-error").textContent="";$("#add-artist-dialog").showModal();}else{state.pendingPersonDetails=null;state.pendingPersonRaw=null;$("#add-person-form").reset();resolveStatus("#add-person-resolve-status");$("#add-person-error").textContent="";$("#add-person-dialog").showModal();} });
$("#resolve-movie").addEventListener("click", resolveMovieDraft); $("#resolve-person").addEventListener("click", resolvePersonDraft); $("#resolve-album").addEventListener("click",resolveAlbumDraft); $("#resolve-artist").addEventListener("click",resolveArtistDraft);
$("#clear-add-movie").addEventListener("click", event => {
  event.preventDefault(); event.stopPropagation(); resetMovieDraft();
  if (!$("#add-dialog").open) $("#add-dialog").showModal();
  $("#add-form").elements.namedItem("title_ru").focus();
});
document.querySelectorAll('[data-save-movie="like"],[data-save-movie="dislike"]').forEach(button => button.addEventListener("click", () => saveMovieDraft(button.dataset.saveMovie)));
document.querySelectorAll('[data-save-album="like"],[data-save-album="dislike"]').forEach(button => button.addEventListener("click",()=>saveAlbumDraft(button.dataset.saveAlbum)));
$("#add-form").addEventListener("input", event => {
  const field = event.target.name;
  if (!state.pendingMovieDetails || !["title_original", "title_ru", "year"].includes(field)) return;
  state.pendingMovieDetails = null; state.pendingMovieSearchField = field;
  $("#add-movie-links").textContent = "—"; renderMovieDraftPreview();
  resolveStatus("#add-resolve-status", "Данные изменены. Нажмите «Актуализировать из TMDB», чтобы выполнить новый поиск.");
});
$("#add-form").addEventListener("submit", event => { event.preventDefault(); saveMovieDraft(); });
$("#clear-add-album").addEventListener("click",event=>{event.preventDefault();event.stopPropagation();resetAlbumDraft();if(!$("#add-album-dialog").open)$("#add-album-dialog").showModal();$("#add-album-form").elements.namedItem("title_original").focus();});
$("#add-album-form").addEventListener("input",event=>{const field=event.target.name;if(!state.pendingAlbumDetails||!["title_original","artists","year"].includes(field))return;state.pendingAlbumDetails=null;state.pendingAlbumSearchField=field;$("#add-album-links").textContent="—";renderAlbumDraftPreview();resolveStatus("#add-album-resolve-status","Данные изменены. Нажмите «Актуализировать из MusicBrainz», чтобы выполнить новый поиск.");});
$("#add-album-form").addEventListener("submit",event=>{event.preventDefault();saveAlbumDraft();});
$("#add-person-form").addEventListener("submit",async event=>{ event.preventDefault(); const form=event.currentTarget,current=formPayload(form); const payload={...(state.pendingPersonDetails||{}),...current,raw_data:state.pendingPersonRaw||compactObject(current)}; try{ const {item}=await request("/api/people",{method:"POST",body:JSON.stringify(payload)});state.interests.push(item);form.reset();state.pendingPersonDetails=null;state.pendingPersonRaw=null;$("#add-person-error").textContent="";$("#add-person-dialog").close();renderPeople();renderInterestOptions();toast("Персона добавлена");}catch(error){$("#add-person-error").textContent=error.message;} });
$("#add-artist-form").addEventListener("submit",async event=>{event.preventDefault();const form=event.currentTarget,current=formPayload(form),payload={...(state.pendingArtistDetails||{}),...current,content_type:"music",raw_data:state.pendingArtistRaw||compactObject(current)};try{const {item}=await request("/api/people",{method:"POST",body:JSON.stringify(payload)});state.interests.push(item);form.reset();state.pendingArtistDetails=null;state.pendingArtistRaw=null;$("#add-artist-error").textContent="";$("#add-artist-dialog").close();renderPeople();renderInterestOptions();toast("Исполнитель добавлен");}catch(error){$("#add-artist-error").textContent=error.message;}});

async function initialize() {
  try { const [{items},meta,trash]=await Promise.all([request("/api/library?content_type=movie"),request("/api/meta"),request("/api/trash")]); state.items=items;state.interests=meta.interests;state.trash=trash.items;state.tmdb=meta.tmdb;state.musicbrainz=meta.musicbrainz||{};state.llm=meta.llm||{};applyContentLabels();renderInterestOptions();renderLibrary();renderPeople();renderTrash(); }
  catch(error){toast(error.message);}
}
initialize();
