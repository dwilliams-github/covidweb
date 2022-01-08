var sequence = 0;
function showPlot(sel,url) {
    sequence += 1;
    const this_sequence = sequence;
    $(sel).LoadingOverlay("show", {
        background: "rgba(51, 51, 51, 0.8)", 
        imageColor: "grey",
        zIndex: 2
    });
    customVegaEmbed(sel, url, {
        abort: function(){return sequence != this_sequence;},
        maxwidth: Math.min($(window).width()-60,640)
    }).then(function(){
        $(sel).LoadingOverlay("hide", true);
    }).catch(console.error);
}
function makePermalink( vid, controls ) {
    var items = ['id='+vid];
    controls.forEach(function(c){
        items.push(c+"="+encodeURIComponent($("#"+c).val()));
    });
    $("#v"+vid+" .permalink a").attr('href',"/?" + items.join("&"));
}
function params(names) {
    return names.map(function(n){return $("#"+n).serialize()}).join("&");
}
function country() {
    const vars = ["selcountry","timecountry"];
    makePermalink(0,vars);
    showPlot("#v0 .vega", "/api/country/graph?"+params(vars));
}
function countryComposite() {
    const vars = ["modecompcountry","timecompcountry"];
    makePermalink(1, vars);
    showPlot("#v1 .vega", "/api/country/composite?"+params(vars));
}
function state() {
    const vars = ["selstate","modestate","timestate"];
    makePermalink(10, vars);
    showPlot("#v10 .vega", "/api/state/graph?"+params(vars));
}
function stateComposite() {
    const vars = ["modecompstate","timecompstate"];
    $("#timecompstate").prop(
        'disabled', 
        ['VB','VP','DB'].indexOf($("#modecompstate").val()) >= 0
    )
    makePermalink(11, vars);
    showPlot("#v11 .vega", "/api/state/composite?"+params(vars));
}
function county() {
    const vars = ["selcounty","timecounty"];
    makePermalink(20, vars);
    showPlot("#v20 .vega", "/api/county/simple?"+params(vars));
}
function countyComposite() {
    const vars = ["modecompcounty","timecompcounty"]
    makePermalink(21, vars);
    showPlot("#v21 .vega", "/api/county/composite?"+params(vars));
}
function renderView(view) {
    switch(view) {
        case 0: return country();
        case 1: return countryComposite();
        case 10: return state();
        case 11: return stateComposite();
        case 20: return county();
        case 21: return countyComposite();
    }
}
function refresh() {
    $("div.menu .selected").each(function(){
        renderView( parseInt($(this).attr("id").substring(1)) );
    });
}
function selectView(vid) {
    $("div.menu .option").removeClass("selected");
    $("#s"+vid).addClass("selected");
    $(".view").addClass("hidden");
    $("#v"+vid ).removeClass('hidden');
    refresh();
}
function toggleMenu() {
    if ($("#menu-icon").css("display") != "block") return;
    $("#menu").toggleClass("menuhide");
}
function defaults(params) {
    var view_id = 0;
    if (params) {
        try {
            params.split("&").forEach(function(p){
                let parts = p.split("=");
                let value = decodeURIComponent(parts[1]);
                if (parts[0] == "id") {
                    view_id = value;
                } else {
                    $("#"+parts[0]).val(value);
                }
            });
        } catch(e) {
            console.log(e);
        }
    }
    return view_id;
}
$(function(){
    let view_id = defaults(this.location.search.substr(1));

    $("#selcountry,#modecompcountry,#selstate,#modecompstate,#selcounty,#modecompcounty").select2({
        width: '18em'
    });
    $("#timecountry,#timecompcountry,#modestate,#timestate,#timecompstate,#timecounty,#timecompcounty").select2({
        width: '10em'
    });
    $("#selcountry,#timecountry").change(country);
    $("#modecompcountry,#timecompcountry").change(countryComposite);
    $("#selstate,#modestate,#timestate").change(state);
    $("#modecompstate,#timecompstate").change(stateComposite);
    $("#selcounty,#timecounty").change(county);
    $("#modecompcounty,#timecompcounty").change(countyComposite);
    $("div.menu .option").click(function(){
        selectView($(this).attr("id").substr(1));
        toggleMenu();
    });
    $("#menu-icon").click(toggleMenu);
    $(".reload").click(refresh);

    selectView(view_id);
});