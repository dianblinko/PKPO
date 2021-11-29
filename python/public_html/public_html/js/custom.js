// about slider
$(function() {
	$('.cr_about_slider').slick({
		dots: true,
		infinite: true,
		speed: 800,
		arrows:false
	});
});
// client slider
$(function() {
	$('.cr_client_slider').slick({
		infinite: true,
		dots: true,
		speed: 800,
		slidesToShow: 3,
		slidesToScroll: 1,
		arrows:false,
		responsive: [
			{
			  breakpoint: 1024,
			  settings: {
				slidesToShow: 3,
				slidesToScroll: 1,
			  }
			},
			{
			  breakpoint: 991,
			  settings: {
				slidesToShow: 2,
				slidesToScroll: 1
			  }
			},
			{
			  breakpoint: 768,
			  settings: {
				slidesToShow: 1,
				slidesToScroll: 1
			  }
			}
		]
	});
});
// choose pie timer
$(function() {
	$('.cr_pie').pieChart({
		trackColor: '#253046',
		barColor: '#de2755',
		lineCap: 'round',
		lineWidth: 5,
		onStep: function (from, to, percent) {
			$(this.element).find('.pie-value').text(Math.round(percent) + '%');
		}
	});
});

// gallery hover effect
$(function() {
  $(' #da-thumbs > li ').each( function() { $(this).hoverdir(); } );
});

// megnific popup
$(function(){
	$('.cr_gallery').each(function() { // the containers for all your galleries
		$(this).magnificPopup({
			delegate: 'a', // the selector for gallery item
			type: 'image',
			gallery: {
			  enabled:true
			},
			 zoom: {
				enabled: true, // By default it's false, so don't forget to enable it

				duration: 300, // duration of the effect, in milliseconds
				easing: 'ease-in-out', // CSS transition easing function

				// The "opener" function should return the element from which popup will be zoomed in
				// and to which popup will be scaled down
				// By defailt it looks for an image tag:
				opener: function(openerElement) {
				  // openerElement is the element on which popup was initialized, in this case its <a> tag
				  // you don't need to add "opener" option if this code matches your needs, it's defailt one.
				  return openerElement.is('a') ? openerElement : openerElement.find('img');
				}
			  }
		});
	});
});
// video section
$(function($){
	$('#cr_video').css("display", "none");
	$('.cr_video_wrapper .cr_video_img .cr_overlay a').on("click", function(e) {
		e.preventDefault();
		$('.ed_video_section .cr_video_img').hide();	
		$('#cr_video').css("display", "block");
		$('#cr_video').attr('src',$('#cr_video').attr('src')+'?rel=0&autoplay=1');
	});
		
});

// revolution slider
$(function($){
	if($('#rev_slider_467_1').length){
		var tpj=jQuery;
		var revapi467;
		tpj(document).ready(function() {
			if(tpj("#rev_slider_467_1").revolution == undefined){
				revslider_showDoubleJqueryError("#rev_slider_467_1");
			}else{
				revapi467 = tpj("#rev_slider_467_1").show().revolution({
					sliderType:"carousel",
					jsFileLocation:"js/revolution",
					sliderLayout:"fullwidth",
					dottedOverlay:"none",
					delay:9000,
					navigation: {
						keyboardNavigation:"off",
						keyboard_direction: "horizontal",
						mouseScrollNavigation:"off",
						mouseScrollReverse:"default",
						onHoverStop:"off",
						arrows: {
							style:"erinyen",
							enable:true,
							hide_onmobile:true,
							hide_under:600,
							hide_onleave:true,
							hide_delay:200,
							hide_delay_mobile:1200,
							tmp:'<div class="tp-title-wrap">  	<div class="tp-arr-imgholder"></div>    <div class="tp-arr-img-over"></div>	<span class="tp-arr-titleholder">{{title}}</span> </div>',
							left: {
								h_align:"left",
								v_align:"center",
								h_offset:30,
								v_offset:0
							},
							right: {
								h_align:"right",
								v_align:"center",
								h_offset:30,
								v_offset:0
							}
						}
						,
						thumbnails: {
							style:"gyges",
							enable:true,
							width:60,
							height:60,
							min_width:60,
							wrapper_padding:0,
							wrapper_color:"transparent",
							wrapper_opacity:"1",
							tmp:'<span class="tp-thumb-img-wrap">  <span class="tp-thumb-image"></span></span>',
							visibleAmount:5,
							hide_onmobile:true,
							hide_under:800,
							hide_onleave:false,
							direction:"horizontal",
							span:false,
							position:"inner",
							space:5,
							h_align:"center",
							v_align:"bottom",
							h_offset:0,
							v_offset:20
						}
					},
					carousel: {
						horizontal_align: "center",
						vertical_align: "center",
						fadeout: "on",
						maxVisibleItems: 1,
						infinity: "on",
						space: 0,
						speed:3000,
						stretch: "off"
					},
					viewPort: {
						enable:true,
						outof:"pause",
						visible_area:"80%",
						presize:false
					},
					responsiveLevels:[1920,1920,1920,1920],
					visibilityLevels:[1920,1920,1920,1920],
					gridwidth:[1920,1920,991,768],
					gridheight:[853,853,853,853],
					lazyType:"none",
					parallax: {
						type:"mouse",
						origo:"slidercenter",
						speed:3000,
						levels:[2,3,4,5,6,7,12,16,10,50,46,47,48,49,50,55],
						type:"mouse",
					},
					spinner:"off",
					stopLoop:"off",
					stopAfterLoops:-1,
					stopAtSlide:-1,
					shuffle:"off",
					autoHeight:"off",
					hideThumbsOnMobile:"on",
					hideSliderAtLimit:0,
					hideCaptionAtLimit:0,
					hideAllCaptionAtLilmit:0,
					debugMode:false,
					fallbacks: {
						simplifyAll:"off",
						nextSlideOnWindowFocus:"off",
						disableFocusListener:false,
					}
				});
			}
		});
	}
});

// wow js
$(function($){
	new WOW().init();
});

// loader js
$( document ).ready(function() {
    setTimeout(function(){ 
		$('body').addClass('site_loaded');
	}, 900);
});

// fixed menu
$(window).scroll(function() {
		if ($(this).scrollTop() > 100) {
            $('.cr_menu_wrapper').addClass('cr_fixed_header');
        } else {
            $('.cr_menu_wrapper').removeClass('cr_fixed_header');
        }
});

// responsive menu
$(function($){
		var w = window.innerWidth;
		if(w <= 991){
			 $(".cr_menu >ul>li").find('.sub-menu').parent().addClass('dropdown');
			 $(".cr_menu >ul>li.dropdown").append('<div class="show-submenu"><i class="fa fa-angle-down"></i></div>');

			$(".cr_menu >ul>li.dropdown").on("click", function() {
				$('.cr_menu >ul>li.dropdown > .show-submenu > i').toggleClass('fa fa-angle-up');
				$('.cr_menu >ul>li.dropdown > .show-submenu > i').toggleClass('fa fa-angle-down');
			});
			
			$('.cr_menu >ul>li.dropdown').click(function() {
				$('.cr_menu >ul>li.dropdown').not($(this)).
				parent().find('.sub-menu').hide();
				$(this).parent().find('.sub-menu').toggle();
			});
			

			$('.sub-menu').parent().hover(function() {
			var menu = $(this).find("ul");var menupos = $(menu).offset();
			if (menupos.left + menu.width() > $(window).width()) {var newpos = -$(menu).width();   
			 menu.css({'left': 'auto','right': '100%'});}
			 }); 
		}
});

// Menu open
$(function($){
	$(".cr_toggle").on("click", function() {
		$(this).toggleClass("cr_toggle_open");
		$(".cr_menu").toggleClass("cr_menu_open");
	});
});

// Contact Form Submition
function checkRequire(formId , targetResp){
		targetResp.html('');
		var email = /^([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\.([a-z]{2,6}(?:\.[a-z]{2})?)$/;
		var url = /(http|ftp|https):\/\/[\w-]+(\.[\w-]+)+([\w.,@?^=%&amp;:\/~+#-]*[\w@?^=%&amp;\/~+#-])?/;
		var image = /\.(jpe?g|gif|png|PNG|JPE?G)$/;
		var mobile = /^[\s()+-]*([0-9][\s()+-]*){6,20}$/;
		var facebook = /^(https?:\/\/)?(www\.)?facebook.com\/[a-zA-Z0-9(\.\?)?]/;
		var twitter = /^(https?:\/\/)?(www\.)?twitter.com\/[a-zA-Z0-9(\.\?)?]/;
		var google_plus = /^(https?:\/\/)?(www\.)?plus.google.com\/[a-zA-Z0-9(\.\?)?]/;
		var check = 0;
		$('#er_msg').remove();
		var target = (typeof formId == 'object')? $(formId):$('#'+formId);
		target.find('input , textarea , select').each(function(){
			if($(this).hasClass('require')){
				if($(this).val().trim() == ''){
					check = 1;
					$(this).focus();
					targetResp.html('You missed out some fields.');
					$(this).addClass('error');
					return false;
				}else{
					$(this).removeClass('error');
				}
			}
			if($(this).val().trim() != ''){
				var valid = $(this).attr('data-valid');
				if(typeof valid != 'undefined'){
					if(!eval(valid).test($(this).val().trim())){
						$(this).addClass('error');
						$(this).focus();
						check = 1;
						targetResp.html($(this).attr('data-error'));
						return false;
					}else{
						$(this).removeClass('error');
					}
				}
			}
		});
		return check;
	}
	$(".submitForm").on("click", function() {
		var _this = $(this);
		var targetForm = _this.closest('form');
		var errroTarget = targetForm.find('.response');
		var check = checkRequire(targetForm , errroTarget);
		if(check == 0){
			var formDetail = new FormData(targetForm[0]);
			$.ajax({
				method : 'post',
				url : 'ajax.php',
				data:formDetail,
				cache:false,
				contentType: false,
				processData: false
			}).done(function(resp){
				console.log(resp);
				if(resp == 1){
					targetForm.find('input').val('');
					targetForm.find('textarea').val('');
					errroTarget.html('<p style="color:green;">Mail has been sent successfully.</p>');
				}else{
					errroTarget.html('<p style="color:red;">Something went wrong please try again latter.</p>');
				}
			});
		}
	});