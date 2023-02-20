from django.urls import path
from . import views
from .views import add_config, ConfigList, run_script, edit_config, delete_config

urlpatterns = [
    path('add_config/', views.add_config,name='add_config'),
    path('config_list/', ConfigList.as_view(), name='config_list'),
    path('edit_config/<int:id>', edit_config, name='edit_config'),
    path('delete_config/<int:id>', delete_config, name='delete_config'),
    path('config_list/', ConfigList.as_view() , name='config_list'),
    path('run_script/', views.run_script, name='run_script'),
    path('', views.home,name='home-page'),
    path('signup/',views.SignupPage,name='signup'),
    path('login/',views.LoginPage,name='login'),
    path('logout/',views.LogoutPage,name='logout'),
]