from django.shortcuts import render
from django.http import HttpResponse
from django.contrib.auth.models import User
from django.views.generic import View
from django.core.paginator import Paginator
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth import logout, authenticate
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.contrib.auth import get_user_model, login
#from .forms import UserRegistrationForm
import os
from django.shortcuts import render
from django.contrib.messages.views import messages
import sys
from subprocess import Popen, PIPE
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
from django.urls import reverse, reverse_lazy
from django.core.mail import send_mail
from django.conf import settings
import smtplib
import ssl
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


lookup_dict = {
'boolean_check': 'R0010',
'char_check': 'R006',
'column_length_check': 'R0025',
'dataset_content_check': 'R0027',
'dataset_equality_check': 'R0024',
'dataset_length_check': 'R0026',
'date_format_check': 'R007',
'decimal_check': 'R0011',
'file_availability_check': 'R0021',
'file_col_count_check': 'R0018',
'file_count_check': 'R0023',
'file_extension': 'R0017',
'file_folder_availability_check': 'R0022',
'file_size': 'R0016',
'header_pattern_check': 'R0019',
'int_check': 'R005',
'lst_values_check': 'R0013',
'not_null': 'R001',
'pattern_check': 'R0020',
'relationship': 'R004',
'timestamp_check': 'R008',
'unique': 'R002',
'varchar_check': 'R009',
}

# Create your views here.
cofig_file_path=str(os.getcwd())+r"\DQF\Projects\Azure\config\Config_Rule.xlsx"
def home(request):
    return render(request,'DQF/home.html')


def SignupPage(request):
    if request.method == 'POST':
        uname = request.POST.get('username')
        email = request.POST.get('email')
        pass1 = request.POST.get('password1')
        pass2 = request.POST.get('password2')

        if pass1 != pass2:
            return HttpResponse("Your password and confrom password are not Same!!")
        else:

            my_user = User.objects.create_user(uname, email, pass1)
            my_user.save()
            return redirect('login')

    return render(request, 'DQF/signup.html')


def LoginPage(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        pass1 = request.POST.get('pass')
        user = authenticate(request, username=username, password=pass1)
        if user is not None:
            login(request, user)
            messages.success(request, f"Hello {user.username}! You have been logged in")
            return redirect('home-page')
        else:
            return HttpResponse("Username or Password is incorrect!!!")

    return render(request, 'DQF/login.html')


def LogoutPage(request):
    logout(request)
    return redirect('login')

@login_required(login_url='login')
def add_config(request):
    args = dict()
    global cofig_file_path
    df = pd.read_excel(cofig_file_path, sheet_name=["DQ_RULE_CONFIG"], index_col=False)
    df = df['DQ_RULE_CONFIG']
    if request.method == "POST":
        form = request.POST
        dict1 = dict(form)
        if len(df) == 0:
            config_id = 1
        else:
            config_id = len(df) + 1
        dict1['config_id'] = [config_id]
        dict1['rule_id'] = lookup_dict[form['rule_name']]
        del dict1['csrfmiddlewaretoken']
        new_record = pd.DataFrame(dict1, index=[0])
        df = pd.concat([df, new_record], ignore_index=True)
        with pd.ExcelWriter(cofig_file_path,mode='a',engine='openpyxl',if_sheet_exists='overlay') as writer:
            df.to_excel(writer,sheet_name='DQ_RULE_CONFIG',index=False)
            writer.save()
        #upload_config()
        messages.success(request, 'Record Added successfully.')
        return HttpResponseRedirect(reverse('config_list'))
    else:
        return render(request, 'DQF/add_config.html')

@login_required(login_url='login')
def edit_config(request, id):
    global cofig_file_path
    df = pd.read_excel(cofig_file_path, sheet_name=["DQ_RULE_CONFIG"], index_col=False)
    df = df['DQ_RULE_CONFIG']
    context = {}
    # if request.method == 'GET':
    #     return render(request, 'DQF/edit_config.html')
    if request.method == "POST":
        form = request.POST
        dict1 = dict(form)
        dict1['config_id'] = [id]
        dict1['rule_id'] = lookup_dict[form['rule_name']]
        del dict1['csrfmiddlewaretoken']
        df.drop(df[df['config_id'] == id].index, inplace=True)
        new_record = pd.DataFrame(dict1, index=[0])
        df = pd.concat([df, new_record], ignore_index=True)
        df = df.sort_values(by=['config_id'])
        with pd.ExcelWriter(cofig_file_path, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            df.to_excel(writer, sheet_name='DQ_RULE_CONFIG', index=False)
            writer.save()
        #upload_config()
        return HttpResponseRedirect(reverse('config_list'))
    else:
        data = df.loc[df['config_id'] == id]
        df11 = data.to_dict('records')
        context['data'] = df11[0]
        return render(request, 'DQF/edit_config.html', context)

@login_required(login_url='login')
def delete_config(request, id):
    # ToDO delete config entry
    global cofig_file_path
    df = pd.read_excel(cofig_file_path, sheet_name=["DQ_RULE_CONFIG"], index_col=False)
    df = df['DQ_RULE_CONFIG']
    df.drop(df[df['config_id'] == id].index, inplace=True)
    with pd.ExcelWriter(cofig_file_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name='DQ_RULE_CONFIG', index=False)
        writer.save()
    #upload_config()
    messages.success(request, 'Record Deleted successfully.')
    return HttpResponseRedirect(reverse('config_list'))

class ConfigList(View):
    template_name = 'DQF/list_config.html'
    paginate_by = 10
    def get(self, request, *args, **kwargs):
        df = pd.read_excel(cofig_file_path, sheet_name=["DQ_RULE_CONFIG"], index_col=False)
        df = df['DQ_RULE_CONFIG']
        df1 = df.to_dict('records')
        paginator = Paginator(df1, 5)  # Show 25 contacts per page.

        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        return render(request, "DQF/list_config.html", {'page_obj': page_obj})

@login_required(login_url='login')
def run_script(request):
    #print(str(os.getcwd())+r"\Projects\Salesforce\config\Config_Rule.xlsx")
    main_project_path = os.getcwd()
    project_path = os.path.join(main_project_path, "DQF/Projects")
    # try:
    #     root, dirs, files = os.walk(project_path).__next__()
    # except StopIteration:
    #     print("No more directories to process.")
    root, dirs, files = os.walk(project_path).__next__()
    if request.method == 'GET':
        return render(request, 'DQF/run_script.html', {"project_data": dirs})
        #, {"project_data": dirs}
    elif request.method == 'POST':
        project = request.POST.get('project')
        source_type = request.POST.get('source_type')
        source_name = request.POST.get('source_name')
        config_location = request.POST.get('config_location')
        args_dict = {'project': project, 'source_type': source_type, 'source_name': source_name, 'config_location': config_location}
        cmd_lst = []
        for key, value in args_dict.items():
            if value:
                if key == "source_name" and value:
                    source_name = source_name.split(',')
                    cmd_lst.append("--" + key)
                    for i in source_name:
                        cmd_lst.append(i.strip())
                else:
                    cmd_lst.append("--" + key)
                    cmd_lst.append(value)
        try:
            print(cmd_lst)
            cmd = Popen([sys.executable, "main.py"] + cmd_lst, shell=False, stdout=PIPE, stderr=PIPE)
            output, error = cmd.communicate()
            if error:
                error = error.decode("utf-8")
                print(error)
                # messages.success(request, error)
                # return HttpResponseRedirect(reverse('run_script'))
                if "ValueError:" in error:
                    str_index = error.index("ValueError:")
                    err_str = error[str_index:len(error)]
                    messages.success(request, err_str)
                    return HttpResponseRedirect(reverse('run_script'))
                elif "KeyError:" in error:
                    str_index = error.index("KeyError:")
                    err_str = error[str_index:len(error)]
                    messages.success(request, err_str)
                    return HttpResponseRedirect(reverse('run_script'))
                elif "Error:" in error:
                    str_index = error.index("Error:")
                    err_str = error[str_index:len(error)]
                    # messages.success(request, err_str)
                    messages.success(request, "Some Error Occurred")
                    return HttpResponseRedirect(reverse('run_script'))
                # else:
                #     return HttpResponse(error)
        except OSError as e:
            print("inside exception", e)
            return HttpResponse(e)
        messages.success(request, 'Script Executed Successfully')
        return HttpResponseRedirect(reverse('run_script'))
        # return HttpResponse('Script Executed Successfully')