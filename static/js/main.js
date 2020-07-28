function showPlot(sel,url) {
    $(sel).LoadingOverlay("show", {background: "rgba(51, 51, 51, 0.8)", imageColor:"grey"});
    vegaEmbed(sel, url)
        .then($(sel).LoadingOverlay("hide"))
        .catch(console.error);
}
function country() {
    showPlot("#v0 .vega", "/api/country/graph?"+$("#selcountry").serialize());
}
function state() {
    showPlot( "#v10 .vega", "/api/state/graph?"+$("#selstate").serialize() + "&" + $("#modestate").serialize() );
}
function stateComposite() {
    showPlot( "#v11 .vega", "/api/state/composite?"+$("#modecompstate").serialize() );
}
function county() {
    showPlot( "#v20 .vega", "/api/county/simple?"+$("#selcounty").serialize() );
}
function countyComposite() {
    showPlot( "#v21 .vega", "/api/county/composite?"+$("#modecompcounty").serialize() );
}
function renderView(view) {
    switch(view) {
        case 0: return country();
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
$(document).ready(function(){
    $("#selcountry,#selstate,#modestate,#modecompstate,#selcounty,#modecompcounty").select2();
    $("#selcountry").change(country);
    $("#selstate,#modestate").change(state);
    $("#modecompstate").change(stateComposite);
    $("#selcounty").change(county);
    $("#modecompcounty").change(countyComposite);
    $("div.menu .option").click(function(){
        $("div.menu .option").removeClass("selected");
        $(this).addClass("selected");
        $(".view").addClass("hidden");
        $("#v" + $(this).attr("id").substr(1)).removeClass('hidden');
        refresh();
    });
    
    refresh();
});