(function ($) {
    var contenedor = ".w3-card";
    var contenedor2 = "#contenedor2";
    var diffAltura = $(contenedor).offset().top + 20;
    function resizeContenedor() {
      var h = $(window).height();
      var w = $(window).width();
      var $contenedor = $(contenedor);
      var $contenedor2 = $(contenedor2);
      if (w > 1024) {
        $contenedor.height(h - diffAltura);
        $contenedor2.height(h - diffAltura);
      }
      // $contenedor.width(w);
    }
    resizeContenedor();
    $(window).resize(resizeContenedor);
  }(jQuery));
  