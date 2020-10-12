function showPlot(sel,url) {
    $(sel).LoadingOverlay("show", {background: "rgba(51, 51, 51, 0.8)", imageColor:"grey"});
    vegaEmbed(sel, url)
        .then($(sel).LoadingOverlay("hide"))
        .catch(console.error);
}
function makePermalink( vid, controls ) {
    var items = ['id='+vid];
    controls.forEach(function(c){
        items.push(c+"="+encodeURIComponent($("#"+c).val()));
    });
    $("#v"+vid+" .permalink a").attr('href',"/?" + items.join("&"));
}
function country() {
    makePermalink(0, ["selcountry"]);
    showPlot("#v0 .vega", "/api/country/graph?"+$("#selcountry").serialize());
}
function countryComposite() {
    makePermalink(1, ["modecompcountry"]);
    showPlot("#v1 .vega", "/api/country/composite?"+$("#modecompcountry").serialize());
}
function state() {
    makePermalink(10, ["selstate","modestate"]);
    showPlot( "#v10 .vega", "/api/state/graph?"+$("#selstate").serialize() + "&" + $("#modestate").serialize() );
}
function stateComposite() {
    makePermalink(11, ["modecompstate"]);
    showPlot( "#v11 .vega", "/api/state/composite?"+$("#modecompstate").serialize() );
}
function county() {
    makePermalink(20, ["selcounty"]);
    showPlot( "#v20 .vega", "/api/county/simple?"+$("#selcounty").serialize() );
}
function countyComposite() {
    makePermalink(21, ["modecompcounty"]);
    showPlot( "#v21 .vega", "/api/county/composite?"+$("#modecompcounty").serialize() );
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
function defaults(params) {
    var view_id = 0;
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
    return view_id;
}
$(function(){
    let view_id = defaults(this.location.search.substr(1));

    $("#selcountry,#modecompcountry,#selstate,#modestate,#modecompstate,#selcounty,#modecompcounty").select2({
        width: '18em'
    });
    $("#selcountry").change(country);
    $("#modecompcountry").change(countryComposite);
    $("#selstate,#modestate").change(state);
    $("#modecompstate").change(stateComposite);
    $("#selcounty").change(county);
    $("#modecompcounty").change(countyComposite);
    $("div.menu .option").click(function(){
        selectView($(this).attr("id").substr(1));
    });

    selectView(view_id);
});