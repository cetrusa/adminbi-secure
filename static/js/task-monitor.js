/**
 * DataZenith Task Monitor - Sistema unificado de monitoreo de tareas asíncronas
 * 
 * Uso:
 *   var monitor = new TaskMonitor({
 *     taskType: 'cubo',                    // Identificador único de la tarea
 *     submitBtnId: 'submitBtnCubo',        // ID del botón de submit
 *     formUrl: '/cubo/',                   // URL del formulario POST
 *     checkTaskUrl: '/check-task-status/', // URL para verificar estado
 *     csrfToken: '...',                    // CSRF Token
 *     modalTitle: 'Procesando Cubo...',    // Título del modal
 *     onComplete: function(response) {},   // Callback al completar (opcional)
 *     extraParams: function() { return {}; }, // Params adicionales (opcional)
 *     serverDatabase: ''                     // Fallback BD desde Django context (opcional)
 *   });
 */
(function (window) {
  'use strict';

  function TaskMonitor(options) {
    if (!options || !options.taskType) {
      console.error('TaskMonitor: taskType es requerido');
      return;
    }

    this.taskType = options.taskType;
    this.submitBtnId = options.submitBtnId;
    this.formUrl = options.formUrl;
    this.checkTaskUrl = options.checkTaskUrl;
    this.csrfToken = options.csrfToken || '';
    this.modalTitle = options.modalTitle || 'Procesando...';
    this.onComplete = options.onComplete || null;
    this.extraParams = options.extraParams || function () { return {}; };
    this.serverDatabase = options.serverDatabase || '';

    // Estado interno
    this.taskIdKey = this.taskType + '_task_id';
    this.startTime = 0;
    this.progressInterval = null;
    this.lastProgressValue = 0;

    // Referencias DOM
    this.progressBar = document.getElementById('progressBar');
    this.progressStage = document.getElementById('progress-stage');
    this.timeInfo = document.getElementById('time-info');
    this.detailedStatus = document.getElementById('detailedStatus');
    this.modal = document.getElementById('processingModal');
    this.bsModal = this.modal ? new bootstrap.Modal(this.modal) : null;
    this.downloadFile = document.getElementById('download_file');
    this.submitBtn = document.getElementById(this.submitBtnId);

    this._init();
  }

  TaskMonitor.prototype._init = function () {
    var self = this;

    // Ocultar descarga al inicio
    if (this.downloadFile) this.downloadFile.className = 'd-none';

    // Listener del botón submit
    if (this.submitBtn) {
      this.submitBtn.addEventListener('click', function (event) {
        event.preventDefault();
        self._onSubmit();
      });
    }

    // Listener del botón de descarga
    if (this.downloadFile) {
      this.downloadFile.addEventListener('click', function () {
        self._onDownload();
      });
    }

    // Verificar tareas pendientes al cargar
    this._checkPendingTasks();
  };

  TaskMonitor.prototype._onSubmit = function () {
    if (this.submitBtn.getAttribute('data-submitted') === 'true') {
      alert('Ya hay una tarea de ' + this.taskType + ' en curso.');
      return;
    }

    // Prioridad: sessionStorage > serverDatabase (Django context) > #database_select (selector)
    var database = window.sessionStorage.getItem('database_name')
                   || this.serverDatabase
                   || (document.getElementById('database_select') && document.getElementById('database_select').value) || '';
    var IdtReporteIni = document.getElementById('IdtReporteIni').value;
    var IdtReporteFin = document.getElementById('IdtReporteFin').value;

    if (!database || !IdtReporteIni || !IdtReporteFin) {
      this.stopMonitoring('Por favor, seleccione la empresa y ambas fechas.', true, 0);
      return;
    }

    console.log('Iniciando generación ' + this.taskType + '...');
    this.submitBtn.setAttribute('data-submitted', 'true');
    this.startTime = new Date().getTime();
    this.lastProgressValue = 0;
    this.updateProgressBar(0, 'Iniciando generación...');
    if (this.bsModal) this.bsModal.show();

    if (this.progressInterval) clearInterval(this.progressInterval);
    var self = this;
    this.progressInterval = setInterval(function () { self._updateElapsedTime(); }, 1000);

    // Construir parámetros
    var params = 'database_select=' + encodeURIComponent(database) +
      '&IdtReporteIni=' + encodeURIComponent(IdtReporteIni) +
      '&IdtReporteFin=' + encodeURIComponent(IdtReporteFin);

    // Agregar batch_size si existe
    var batchEl = document.getElementById('batch_size');
    if (batchEl) {
      params += '&batch_size=' + encodeURIComponent(batchEl.value);
    }

    // Agregar parámetros extra
    var extra = this.extraParams();
    for (var key in extra) {
      if (extra.hasOwnProperty(key)) {
        params += '&' + encodeURIComponent(key) + '=' + encodeURIComponent(extra[key]);
      }
    }

    // Enviar solicitud AJAX
    var xhr = new XMLHttpRequest();
    xhr.responseType = 'text';
    xhr.open('POST', this.formUrl, true);
    xhr.setRequestHeader('X-CSRFToken', this.csrfToken);
    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');

    var self = this;
    xhr.onreadystatechange = function () {
      if (this.readyState === XMLHttpRequest.DONE) {
        try {
          var response = JSON.parse(this.responseText);
          self._handleServerResponse(this.status, response);
        } catch (e) {
          console.error('Error procesando respuesta:', e, this.responseText);
          self.stopMonitoring('Error al procesar la respuesta del servidor.');
        }
      }
    };
    xhr.send(params);
  };

  TaskMonitor.prototype._handleServerResponse = function (status, response) {
    if (status === 200 && typeof response === 'object' && 'success' in response) {
      if (response.success) {
        console.log('Tarea ' + this.taskType + ' iniciada. Task ID:', response.task_id);
        window.sessionStorage.setItem(this.taskIdKey, response.task_id);
        this.updateProgressBar(5, 'Tarea iniciada. Esperando procesamiento...');
        var self = this;
        setTimeout(function () { self.checkTaskStatus(); }, 1000);
      } else {
        this.stopMonitoring('Error al iniciar: ' + (response.error_message || 'Error desconocido'));
      }
    } else {
      this.stopMonitoring('Error en la solicitud, código: ' + status);
    }
  };

  TaskMonitor.prototype.checkTaskStatus = function () {
    var taskId = window.sessionStorage.getItem(this.taskIdKey);
    if (!taskId) {
      this.stopMonitoring(null, false);
      return;
    }

    var timestamp = new Date().getTime();
    var xhr = new XMLHttpRequest();
    xhr.responseType = 'text';
    xhr.open('POST', this.checkTaskUrl + '?t=' + timestamp, true);
    xhr.setRequestHeader('X-CSRFToken', this.csrfToken);
    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');

    var self = this;
    xhr.onreadystatechange = function () {
      if (this.readyState === XMLHttpRequest.DONE) {
        try {
          var response = JSON.parse(this.responseText);
          self._handleTaskStatus(this.status, response);
        } catch (e) {
          console.error('Error check_task_status:', e);
          self.stopMonitoring('Error al verificar estado de la tarea.');
        }
      }
    };
    xhr.send('task_id=' + encodeURIComponent(taskId));
  };

  TaskMonitor.prototype._handleTaskStatus = function (status, response) {
    if (status !== 200 || typeof response !== 'object' || !('status' in response)) {
      this.stopMonitoring('Error al comprobar estado, código: ' + status);
      return;
    }

    // Actualizar progreso
    var progressValue = response.progress !== undefined
      ? response.progress
      : parseInt(this.progressBar.getAttribute('aria-valuenow')) || 5;
    var stageText = response.stage || (response.meta && response.meta.stage) || 'Procesando...';
    this.updateProgressBar(progressValue, stageText);
    this._updateEtaInfo(response.eta);

    // Detalles adicionales
    if (this.detailedStatus) {
      this.detailedStatus.innerText = '';
      if (response.meta) {
        var detailText = '';
        if (response.meta.current_step && response.meta.total_steps) {
          detailText += 'Paso ' + response.meta.current_step + ' de ' + response.meta.total_steps + '. ';
        }
        if (response.meta.records_processed !== undefined && response.meta.total_records_estimate !== undefined) {
          detailText += response.meta.records_processed.toLocaleString() + ' de ' +
            response.meta.total_records_estimate.toLocaleString() + ' registros. ';
        } else if (response.meta.records_processed !== undefined) {
          detailText += response.meta.records_processed.toLocaleString() + ' registros procesados. ';
        }
        this.detailedStatus.innerText = detailText;
      }
    }

    // Estado final
    var taskStatus = response.status.toLowerCase();
    var self = this;

    if (taskStatus === 'completed' || taskStatus === 'finished' || taskStatus === 'success') {
      this.updateProgressBar(100, 'Proceso completado exitosamente');
      if (this.downloadFile) this.downloadFile.className = 'd-flex';

      var message = '¡Proceso completado exitosamente!';
      if (response.result) {
        if (response.result.message) message = response.result.message;
        if (response.result.metadata && response.result.metadata.total_records !== undefined) {
          message += ' Se procesaron ' + response.result.metadata.total_records.toLocaleString() + ' registros.';
        }
        if (response.result.execution_time !== undefined) {
          message += ' Tiempo total: ' + response.result.execution_time.toFixed(1) + ' seg.';
        }
      }
      if (this.onComplete) this.onComplete(response);
      this.stopMonitoring(message, true, 1000);

    } else if (taskStatus === 'failed') {
      this.updateProgressBar(100, 'Proceso fallido');
      var errorMsg = 'Error en el proceso';
      if (response.error_message) errorMsg = response.error_message;
      else if (response.error) errorMsg = response.error;
      else if (response.result && typeof response.result === 'string') errorMsg = response.result;
      else if (response.result && response.result.error_message) errorMsg = response.result.error_message;
      else if (response.result && response.result.error) errorMsg = response.result.error;
      else if (response.result && response.result.message) errorMsg = response.result.message;
      else if (response.meta && response.meta.error) errorMsg = response.meta.error;
      else if (response.traceback) errorMsg = response.traceback.split('\n').slice(-2)[0];
      this.stopMonitoring('Error en el proceso: ' + errorMsg, true, 1000);

    } else if (taskStatus === 'partial_success') {
      this.updateProgressBar(100, 'Completado con advertencias');
      if (this.downloadFile) this.downloadFile.className = 'd-flex';
      var partialMsg = response.result && response.result.message
        ? response.result.message
        : 'El archivo se generó con algunas advertencias.';
      this.stopMonitoring(partialMsg, true, 1000);

    } else {
      // En progreso
      if (response.meta && response.meta.file_ready && progressValue >= 80) {
        if (this.downloadFile) this.downloadFile.className = 'd-flex';
      }
      setTimeout(function () { self.checkTaskStatus(); }, 3000);
    }
  };

  TaskMonitor.prototype.updateProgressBar = function (value, stageText) {
    value = Math.max(0, Math.min(100, Number(value) || 0));
    if (value < this.lastProgressValue) {
      value = this.lastProgressValue;
    } else {
      this.lastProgressValue = value;
    }
    if (this.progressBar) {
      this.progressBar.style.width = value + '%';
      this.progressBar.setAttribute('aria-valuenow', value);
      this.progressBar.textContent = value.toFixed(0) + '%';
      if (value < 100) {
        this.progressBar.className = 'progress-bar progress-bar-striped progress-bar-animated bg-primary';
      } else {
        this.progressBar.className = 'progress-bar bg-primary';
      }
    }
    if (stageText && this.progressStage) {
      this.progressStage.textContent = stageText;
    }
  };

  TaskMonitor.prototype._updateElapsedTime = function () {
    if (!this.timeInfo) return;
    var elapsedText = 'Tiempo transcurrido: ';
    if (this.startTime > 0) {
      var elapsedSeconds = Math.floor((new Date().getTime() - this.startTime) / 1000);
      var minutes = Math.floor(elapsedSeconds / 60);
      var seconds = elapsedSeconds % 60;
      elapsedText += (minutes > 0 ? minutes + ' min ' : '') + seconds + ' seg';
    } else {
      elapsedText += '0 seg';
    }
    var currentText = this.timeInfo.textContent;
    var etaPart = currentText.includes('|') ? ' |' + currentText.split('|')[1] : '';
    this.timeInfo.textContent = elapsedText + etaPart;
  };

  TaskMonitor.prototype._updateEtaInfo = function (etaSeconds) {
    if (!this.timeInfo) return;
    var currentElapsed = this.timeInfo.textContent.split('|')[0].trim();
    if (etaSeconds !== undefined && etaSeconds !== null && etaSeconds > 0) {
      var etaMin = Math.floor(etaSeconds / 60);
      var etaSec = Math.floor(etaSeconds % 60);
      var etaText = (etaMin > 0 ? etaMin + ' min ' : '') + etaSec + ' seg';
      this.timeInfo.textContent = currentElapsed + ' | Tiempo restante estimado: ' + etaText;
    } else {
      this.timeInfo.textContent = currentElapsed;
    }
  };

  TaskMonitor.prototype.stopMonitoring = function (alertMessage, showAlert, delay) {
    showAlert = showAlert !== undefined ? showAlert : true;
    delay = delay || 0;

    if (this.progressInterval) clearInterval(this.progressInterval);
    this.progressInterval = null;
    this.startTime = 0;

    // Color final de la barra
    var finalClass = 'progress-bar bg-primary';
    if (alertMessage) {
      var msg = alertMessage.toLowerCase();
      if (msg.includes('exitosamente') || msg.includes('completado')) {
        finalClass = 'progress-bar bg-success';
      } else if (msg.includes('error') || msg.includes('fallido')) {
        finalClass = 'progress-bar bg-danger';
      } else if (msg.includes('advertencia')) {
        finalClass = 'progress-bar bg-warning';
      }
    }
    if (this.progressBar) this.progressBar.className = finalClass;

    var self = this;
    var finalize = function () {
      if (self.bsModal) self.bsModal.hide();
      if (showAlert && alertMessage) alert(alertMessage);
      if (self.submitBtn) self.submitBtn.setAttribute('data-submitted', 'false');
      window.sessionStorage.removeItem(self.taskIdKey);
      self.updateProgressBar(0, 'Listo para iniciar...');
      self.lastProgressValue = 0;
      if (self.timeInfo) self.timeInfo.textContent = 'Tiempo transcurrido: 0 seg';
      if (self.detailedStatus) self.detailedStatus.innerText = '';
    };

    if (delay > 0) setTimeout(finalize, delay);
    else finalize();
  };

  TaskMonitor.prototype._onDownload = function () {
    var downloadLink = document.getElementById('download_link');
    var delay = 3000;
    if (downloadLink && downloadLink.hasAttribute('data-filesize')) {
      var fileSize = parseInt(downloadLink.getAttribute('data-filesize'));
      if (!isNaN(fileSize) && fileSize > 50 * 1024 * 1024) delay = 20000;
      else if (!isNaN(fileSize) && fileSize > 10 * 1024 * 1024) delay = 10000;
    }
    var self = this;
    setTimeout(function () {
      self._deleteFile();
      setTimeout(function () { location.reload(); }, 10000);
    }, delay);
  };

  TaskMonitor.prototype._deleteFile = function () {
    var downloadLink = document.getElementById('download_link');
    if (!downloadLink || !downloadLink.href) return;
    var url = new URL(downloadLink.href, window.location.origin);
    var fileName = url.searchParams.get('file_name');
    if (!fileName) return;

    var xhr = new XMLHttpRequest();
    xhr.responseType = 'text';
    xhr.open('POST', this.formUrl.replace(/[^/]+\/$/, '') + 'delete_file/', true);
    xhr.setRequestHeader('X-CSRFToken', this.csrfToken);
    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    xhr.send('file_name=' + encodeURIComponent(fileName));
  };

  TaskMonitor.prototype._checkPendingTasks = function () {
    var self = this;
    document.addEventListener('DOMContentLoaded', function () {
      var pendingTaskId = window.sessionStorage.getItem(self.taskIdKey);
      if (pendingTaskId) {
        if (confirm('Detectamos una tarea de ' + self.taskType + ' pendiente. ¿Desea continuar monitoreando?')) {
          if (self.submitBtn) self.submitBtn.setAttribute('data-submitted', 'true');
          if (self.bsModal) self.bsModal.show();
          self.updateProgressBar(5, 'Reanudando monitoreo...');
          self.startTime = new Date().getTime();
          self.lastProgressValue = 0;
          if (self.progressInterval) clearInterval(self.progressInterval);
          self.progressInterval = setInterval(function () { self._updateElapsedTime(); }, 1000);
          self.checkTaskStatus();
        } else {
          window.sessionStorage.removeItem(self.taskIdKey);
          if (self.submitBtn) self.submitBtn.setAttribute('data-submitted', 'false');
        }
      } else {
        if (self.submitBtn) self.submitBtn.setAttribute('data-submitted', 'false');
      }
    });
  };

  // Exponer globalmente
  window.TaskMonitor = TaskMonitor;

})(window);
