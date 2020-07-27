function country() {
    $("#v0 .vega").addClass("shade");
    vegaEmbed( "#v0 .vega", "/api/country/graph?"+$("#selcountry").serialize() )
    .then($("#v0 .vega").removeClass("shade"))
    .catch(console.error);
}
function state() {
    $("#v10 .vega").addClass("shade");
    vegaEmbed( "#v10 .vega", "/api/state/graph?"+$("#selstate").serialize() + "&" + $("#modestate").serialize() )
    .then($("#v10 .vega").removeClass("shade"))
    .catch(console.error);
}
function renderView(view) {
    switch(view) {
        case 0: return country();
        case 10: return state();
    }
}
function refresh() {
    $("div.menu .selected").each(function(){
        renderView( parseInt($(this).attr("id").substring(1)) );
    });
}
$(document).ready(function(){
    $("#selcountry,#selstate,#modestate,#modecompstate").select2();
    $("#selcountry").change(country);
    $("#selstate,#modestate").change(state);
    refresh();
    $("div.menu .option").click(function(){
        $("div.menu .option").removeClass("selected");
        $(this).addClass("selected");
        $(".view").addClass("hidden");
        $("#v" + $(this).attr("id").substr(1)).removeClass('hidden');
        refresh();
    });
});